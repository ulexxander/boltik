package boltik

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func makeBolt(t *testing.T) (*bbolt.DB, error) {
	dbf := fmt.Sprintf("%s/db.bolt", t.TempDir())
	return bbolt.Open(dbf, 0700, bbolt.DefaultOptions)
}

func makeFactory(t *testing.T, codec Codec) (func(name []byte) *Box, error) {
	bdb, err := makeBolt(t)
	if err != nil {
		return nil, err
	}

	return NewBoxFactory(bdb, codec), nil
}

func TestBasic(t *testing.T) {
	r := require.New(t)
	bdb, err := makeBolt(t)
	r.NoError(err)

	b := NewBox(bdb, []byte("b1"), nil)

	k := []byte("hello")
	v := []byte("true")
	err = b.Put(k, v)
	r.NoError(err)

	gotv := b.Get(k)
	r.Equal(v, gotv)

	err = b.Delete(k)
	r.NoError(err)

	delv := b.Get(k)
	r.Nil(delv)
}

func TestNested(t *testing.T) {
	r := require.New(t)
	bdb, err := makeBolt(t)
	r.NoError(err)

	bf := NewBoxFactory(bdb, nil)

	tt := []struct {
		bucket []byte
		k      []byte
		k2     []byte
		v      []byte
	}{
		{
			bucket: []byte("parentt"),
			k:      []byte("k1"),
			v:      []byte("vvv1"),
		},
		{
			bucket: []byte("middle"),
			k:      []byte("k2"),
			k2:     []byte("k1"), // key of root
			v:      []byte("v2"),
		},
		{
			bucket: []byte("chiil"),
			k:      []byte("k3"),
			k2:     []byte("k2"), // parent key
			v:      []byte("v3123"),
		},
		{
			bucket: []byte("uu_deeep"),
			k:      []byte("k4"),
			k2:     []byte("k1"), // key of root
			v:      []byte("v4asdfaslkd"),
		},
	}

	var b *Box
	for _, item := range tt {
		b = bf(item.bucket)

		ne := b.Get([]byte("not_exists"))
		r.Nil(ne)

		err := b.Put(item.k, item.v)
		r.NoError(err)

		gotv := b.Get(item.k)
		r.Equal(item.v, gotv)

		parentv := b.Get(item.k2)
		r.Nil(parentv)

		allv := b.GetAll()
		r.NotContains(allv, parentv)
	}
}

func TestWithCodec(t *testing.T) {
	r := require.New(t)
	bdb, err := makeBolt(t)
	r.NoError(err)

	b := NewBox(bdb, []byte("with_codec"), NewCodecJSON())
	k := []byte("k_k_k")
	v := map[string]int{
		"first":  1,
		"second": 20,
		"third":  100500,
	}

	err = b.PutEncoded(k, v)
	r.NoError(err)

	var gotv map[string]int
	err = b.GetDecoded(k, &gotv)
	r.NoError(err)
	r.Equal(v, gotv)
}

func TestSequence(t *testing.T) {
	r := require.New(t)
	bf, err := makeFactory(t, nil)
	r.NoError(err)

	b := bf([]byte("sequence"))

	for i := 0; i < 5; i++ {
		ns, err := b.NextSequence()
		r.NoError(err)
		r.Equal(uint64(i+1), ns)
	}
}

func TestGetAll(t *testing.T) {
	r := require.New(t)
	bf, err := makeFactory(t, nil)
	r.NoError(err)

	tt := []struct {
		k []byte
		v []byte
	}{
		{
			k: []byte("k_first"),
			v: []byte("abc"),
		},
		{
			k: []byte("k_second"),
			v: []byte("efg"),
		},
		{
			k: []byte("k_third"),
			v: []byte("owreosdSFasdjf32"),
		},
	}

	b := bf([]byte("get_all"))

	for i, tc := range tt {
		err := b.Put(tc.k, tc.v)
		r.NoError(err)

		all := b.GetAll()
		r.Len(all, i+1)
		r.Equal(tc.v, all[i])
	}
}

func TestGetAllDecoded(t *testing.T) {
	r := require.New(t)
	bf, err := makeFactory(t, NewCodecJSON())
	r.NoError(err)

	tt := []struct {
		k []byte
		v map[string]string
	}{
		{
			k: []byte("123_k_first"),
			v: map[string]string{
				"abc": "some stuff",
			},
		},
		{
			k: []byte("123_k_second"),
			v: map[string]string{
				"thing": "asdfffff    aa  ---234",
			},
		},
		{
			k: []byte("123_k_third"),
			v: map[string]string{
				"cool": "yes",
			},
		},
	}

	b := bf([]byte("get_all_decoded"))

	for i, tc := range tt {
		err := b.PutEncoded(tc.k, tc.v)
		r.NoError(err)

		var items []map[string]string
		err = b.GetAllDecoded(&items)
		r.NoError(err)

		r.Len(items, i+1)
		r.Equal(tc.v, items[i])
	}
}

func TestPrefixScan(t *testing.T) {
	r := require.New(t)
	bf, err := makeFactory(t, nil)
	r.NoError(err)

	p1 := []byte("prefix1")

	type c struct {
		k []byte
		v []byte
		e [][]byte
	}

	tt := []c{
		{
			k: []byte("noprefix"),
			v: []byte("abc"),
			e: nil,
		},
		{
			k: []byte("prefix1.first"),
			v: []byte("efg"),
			e: [][]byte{[]byte("efg")},
		},
		{
			k: []byte("prefix1.second"),
			v: []byte("e14g"),
			e: [][]byte{[]byte("efg"), []byte("e14g")},
		},
		{
			k: []byte("prefix2.first"),
			v: []byte("15g"),
			e: [][]byte{[]byte("efg"), []byte("e14g")},
		},
	}

	b := bf([]byte("prefix_scan"))

	for i, tc := range tt {
		err := b.Put(tc.k, tc.v)
		r.NoError(err)

		scanned := b.PrefixScan(p1)
		r.Equalf(tc.e, scanned, "on %d", i)
	}
}
