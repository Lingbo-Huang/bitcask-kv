package bitcask_go

import (
	"bitcask-go/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDB_NewIterator(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	// 1. 数据库中没有数据
	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, false, iterator.Valid())
}

func TestDB_Iterator_One_Value(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-2")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 2. 数据库中有一条数据
	err = db.Put(utils.GetTestKey(10), utils.GetTestKey(10))
	assert.Nil(t, err)
	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, true, iterator.Valid())
	//t.Log(string(iterator.Key()))
	assert.Equal(t, utils.GetTestKey(10), iterator.Key())
	val, err := iterator.Value()
	assert.Nil(t, err)
	//t.Log(string(iterator.Key()))
	//t.Log(string(val))
	assert.Equal(t, utils.GetTestKey(10), val)
}

func TestDB_Iterator_Multi_Value(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-3")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destoryDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 3. 数据库中有多条数据
	err = db.Put([]byte("hlb"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("idl"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("abc"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("ljx"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("wjl"), utils.RandomValue(10))
	assert.Nil(t, err)

	// 正向迭代
	iter1 := db.NewIterator(DefaultIteratorOptions)
	// btreeIterator 里的 values 用的是 btree 的 items, 它实现了 less， 所以是递增有序的
	for iter1.Rewind(); iter1.Valid(); iter1.Next() {
		//t.Log("key = ", string(iter1.Key()))
		assert.NotNil(t, iter1.Key())
	}
	// 用 Seek 选取一个迭代的起始位置
	iter1.Rewind()
	for iter1.Seek([]byte("h")); iter1.Valid(); iter1.Next() {
		//t.Log(string(iter1.Key()))
		assert.NotNil(t, iter1.Key())
	}

	// 反向迭代
	rev_opts1 := DefaultIteratorOptions
	rev_opts1.Reverse = true
	iter2 := db.NewIterator(rev_opts1)
	for iter2.Rewind(); iter2.Valid(); iter2.Next() {
		//t.Log("key = ", string(iter2.Key()))
		assert.NotNil(t, iter2.Key())
	}
	// 用 Seek 选取一个迭代的起始位置
	iter1.Rewind()
	for iter2.Seek([]byte("h")); iter2.Valid(); iter2.Next() {
		//t.Log(string(iter2.Key()))
		assert.NotNil(t, iter2.Key())
	}

	// 指定了 prefix 进行迭代
	prefOpts := DefaultIteratorOptions
	prefOpts.Prefix = []byte("l")
	iter3 := db.NewIterator(prefOpts)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		//t.Log(string(iter3.Key()))
		assert.NotNil(t, iter3.Key())
	}
}
