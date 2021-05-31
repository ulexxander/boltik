package boltik

import (
	"bytes"
	"errors"

	"go.etcd.io/bbolt"
)

var (
	ErrNoBucket = errors.New("bucket does not exist")
	ErrNoCodec  = errors.New("no codec defined")
)

type Box struct {
	parent *Box
	db     *bbolt.DB
	name   []byte
	codec  Codec
}

func NewBox(db *bbolt.DB, name []byte, codec Codec) *Box {
	return &Box{
		parent: nil,
		db:     db,
		name:   name,
		codec:  codec,
	}
}

func NewBoxFactory(db *bbolt.DB, codec Codec) func(name []byte) *Box {
	return func(name []byte) *Box {
		return NewBox(db, name, codec)
	}
}

func (b *Box) Get(k []byte) []byte {
	var v []byte

	b.db.View(func(t *bbolt.Tx) error {
		bi, err := b.TxBucket(t, false)
		if err != nil {
			return err
		}

		v = bi.Get(k)
		return nil
	})

	return v
}

func (b *Box) GetDecoded(k []byte, out interface{}) error {
	if b.codec == nil {
		return ErrNoCodec
	}
	v := b.Get(k)
	return b.codec.Unmarshal(v, out)
}

func (b *Box) Put(k, v []byte) error {
	return b.db.Update(func(t *bbolt.Tx) error {
		bi, err := b.TxBucket(t, true)
		if err != nil {
			return err
		}
		return bi.Put(k, v)
	})
}

func (b *Box) PutEncoded(k []byte, v interface{}) error {
	if b.codec == nil {
		return ErrNoCodec
	}
	encoded, err := b.codec.Marshal(v)
	if err != nil {
		return err
	}
	return b.Put(k, encoded)
}

func (b *Box) Delete(k []byte) error {
	return b.db.Update(func(t *bbolt.Tx) error {
		bi, err := b.TxBucket(t, false)
		if err != nil {
			return err
		}
		return bi.Delete(k)
	})
}

func (b *Box) DeleteReturning(k []byte) ([]byte, error) {
	toBeDeleted := b.Get(k)
	if err := b.Delete(k); err != nil {
		return nil, err
	}
	return toBeDeleted, nil
}

func (b *Box) DeleteReturningDecoded(k []byte, out interface{}) error {
	if b.codec == nil {
		return ErrNoCodec
	}
	deleted, err := b.DeleteReturning(k)
	if err != nil {
		return err
	}
	return b.codec.Unmarshal(deleted, out)
}

func (b *Box) GetAll() [][]byte {
	var data [][]byte

	b.db.View(func(t *bbolt.Tx) error {
		bi, err := b.TxBucket(t, false)
		if err != nil {
			return err
		}

		c := bi.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			data = append(data, v)
		}

		return nil
	})

	return data
}

func (b *Box) GetAllDecoded(out interface{}) error {
	if b.codec == nil {
		return ErrNoCodec
	}
	data := b.GetAll()
	joined := b.codec.Join(data)
	return b.codec.Unmarshal(joined, out)
}

func (b *Box) PrefixScan(prefix []byte) [][]byte {
	var data [][]byte

	b.db.View(func(t *bbolt.Tx) error {
		bkt, err := b.TxBucket(t, false)
		if err != nil {
			return err
		}

		c := bkt.Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			data = append(data, v)
		}

		return nil
	})

	return data
}

func (b *Box) NextSequence() (uint64, error) {
	var seq uint64

	err := b.db.Update(func(t *bbolt.Tx) error {
		bkt, err := b.TxBucket(t, true)
		if err != nil {
			return err
		}
		seq, err = bkt.NextSequence()
		return err
	})

	return seq, err
}

func (b *Box) Nested(name []byte) *Box {
	return &Box{
		parent: b,
		db:     b.db,
		name:   name,
	}
}

func (b *Box) TxBucket(t *bbolt.Tx, createIfNX bool) (*bbolt.Bucket, error) {
	root := b
	paths := [][]byte{b.name}
	for root.parent != nil {
		root = root.parent
		paths = append([][]byte{root.name}, paths...)
	}

	var bkt *bbolt.Bucket
	var err error

	if createIfNX {
		bkt, err = t.CreateBucketIfNotExists(paths[0])
		if err != nil {
			return nil, err
		}
	} else {
		bkt = t.Bucket(paths[0])
	}

	for _, path := range paths[1:] {
		if createIfNX {
			bkt, err = bkt.CreateBucketIfNotExists(path)
			if err != nil {
				return nil, err
			}
		} else {
			bkt = bkt.Bucket(path)
		}
	}

	if bkt == nil {
		return nil, ErrNoBucket
	}

	return bkt, nil
}
