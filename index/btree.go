package index

import (
	"bitcask-go/data"
	"bytes"
	"github.com/google/btree"
	"sort"
	"sync"
)

// BTree 索引，封装 google 的btree ku
// https://github.com/google/btree
type BTree struct {
	// Write operations are not safe for concurrent mutation by multiple
	// goroutines, but Read operations are.(google/btree)
	tree *btree.BTree
	// 多线程环境访问存储引擎，需要加锁
	lock *sync.RWMutex
}

// NewBTree 初始化 BTree 索引结构
func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32), // 传入 degree 参数表示叶子节点个数
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	oldItem := bt.tree.Delete(it)
	if oldItem == nil {
		return false
	}
	return true
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBtreeIterator(bt.tree, reverse)
}

// BTree 索引迭代器
type btreeIterator struct {
	currIndex int     // 当前遍历到数组的哪个下标
	reverse   bool    // 是否是反向遍历
	values    []*Item // key 和位置索引信息
}

func newBtreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	// 将所有的数据存放到数组中
	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}

	if reverse { // 从大到小
		tree.Descend(saveValues) // 从大到小取出索引信息
	} else {
		tree.Ascend(saveValues) //从小到大排列取出索引信息

	}

	return &btreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

func (bti *btreeIterator) Rewind() {
	bti.currIndex = 0
}

func (bti *btreeIterator) Seek(key []byte) {
	if bti.reverse { // 反序,就返回第一个小于它的元素
		sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
	} else { // 返回第一个大于它的元素
		sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}

}

func (bti *btreeIterator) Next() {
	bti.currIndex += 1
}

func (bti *btreeIterator) Valid() bool {
	return bti.currIndex < len(bti.values)
}

func (bti *btreeIterator) Key() []byte {
	return bti.values[bti.currIndex].key
}

func (bti *btreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.currIndex].pos
}

func (bti *btreeIterator) Close() {
	bti.values = nil
}
