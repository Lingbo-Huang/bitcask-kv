package index

import (
	"bitcask-go/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)
	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.True(t, res3)

	pos2 := bt.Get([]byte("a"))
	// t.Log(pos2) // 显示 pos2 的内容
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(3), pos2.Offset)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	res2 := bt.Delete(nil)
	assert.True(t, res2)

	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.True(t, res3)

	res4 := bt.Delete([]byte("a"))
	assert.True(t, res4)
}

func TestBTree_Iterator(t *testing.T) {
	bt1 := NewBTree()

	// 1. BTree 为空的情况
	iter1 := bt1.Iterator(false)
	//t.Log(iter1.Valid())
	assert.Equal(t, false, iter1.Valid())

	// 2. BTree 有数据的情况
	bt1.Put([]byte("abcd"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter2 := bt1.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	//t.Log(iter2.Key())
	//t.Log(iter2.Value())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())

	// 3. BTree 有多条数据
	bt1.Put([]byte("hlb"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("ljx"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("love"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter3 := bt1.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		//t.Log("key = ", string(iter3.Key()))
		assert.NotNil(t, iter3.Key())
	}

	iter4 := bt1.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		//t.Log("key = ", string(iter4.Key()))
		assert.NotNil(t, iter4.Key())
	}

	// 4. 测试 seek
	iter5 := bt1.Iterator(false)
	for iter5.Seek([]byte("b")); iter5.Valid(); iter5.Next() {
		//t.Log(string(iter5.Key()))
		assert.NotNil(t, iter5.Key())
	}
	//t.Log(string(iter5.Key()))

	// 5. 反向遍历的 seek
	iter6 := bt1.Iterator(true)
	iter6.Seek([]byte("lo"))
	t.Log(string(iter6.Key()))
}
