package index

import (
	"bitcask-go/data"
	"go.etcd.io/bbolt"
	"path/filepath"
)

const bptreeIndexFileName = "bptree-index"

var indexBucketName = []byte("bitcask-index")

// BPlusTree B+ 树索引
// 主要封装了 go.etcd.io/bbolt 库
type BPlusTree struct {
	tree *bbolt.DB
}

// NewBPlusTree 初始化 B+ 树索引
func NewBPlusTree(dirPath string, syncWrites bool) *BPlusTree {
	opts := bbolt.DefaultOptions
	opts.NoSync = !syncWrites
	bptree, err := bbolt.Open(filepath.Join(dirPath, bptreeIndexFileName), 0644, opts)
	if err != nil {
		panic("failed to open bptree")
	}

	// 创建对应的 bucket
	if err := bptree.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("failed to create bucket in bptree")
	}

	return &BPlusTree{tree: bptree}
}

func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	var oldVal []byte
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		oldVal = bucket.Get(key)
		return bucket.Put(key, data.EncodeLogRecordPos(pos))
	}); err != nil {
		panic("failed to put value in bptree")
	}
	if len(oldVal) == 0 {
		return nil
	}
	return data.DecodeLogRecordPos(oldVal)
}

func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			pos = data.DecodeLogRecordPos(value)
		}
		return nil
	}); err != nil {
		panic("failed to get value in bptree")
	}
	return pos
}

func (bpt *BPlusTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	var oldVal []byte
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		if oldVal = bucket.Get(key); len(oldVal) != 0 {
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("failed to delete value in bptree")
	}
	if len(oldVal) == 0 {
		return nil, false
	}
	return data.DecodeLogRecordPos(oldVal), true
}

func (bpt *BPlusTree) Size() int {
	var size int
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("failed to get size in bptree")
	}
	return size
}

func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return newBptreeIterator(bpt.tree, reverse)
}

func (bpt *BPlusTree) Close() error {
	return bpt.tree.Close()
}

// B+树迭代器
type bptreeIterator struct {
	tx        *bbolt.Tx
	cursor    *bbolt.Cursor
	reverse   bool
	currKey   []byte
	currValue []byte
}

func newBptreeIterator(tree *bbolt.DB, reverse bool) *bptreeIterator {
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to begin a transaction")
	}
	bpi := &bptreeIterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		reverse: reverse,
	}
	bpi.Rewind()
	return bpi
}

func (bpi *bptreeIterator) Rewind() {
	if bpi.reverse {
		bpi.currKey, bpi.currValue = bpi.cursor.Last()
	} else {
		bpi.currKey, bpi.currValue = bpi.cursor.First()
	}
}

func (bpi *bptreeIterator) Seek(key []byte) {
	bpi.currKey, bpi.currValue = bpi.cursor.Seek(key)
}

func (bpi *bptreeIterator) Next() {
	if bpi.reverse {
		bpi.currKey, bpi.currValue = bpi.cursor.Prev()
	} else {
		bpi.currKey, bpi.currValue = bpi.cursor.Next()
	}
}

func (bpi *bptreeIterator) Valid() bool {
	return len(bpi.currKey) != 0
}

func (bpi *bptreeIterator) Key() []byte {
	return bpi.currKey
}

func (bpi *bptreeIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(bpi.currValue)
}

func (bpi *bptreeIterator) Close() {
	_ = bpi.tx.Rollback()
}
