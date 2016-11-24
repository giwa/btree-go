package btree

// Item represents a single object in the tree
type Item interface {
	// Less tests whether the current item is less than the given argument.
	//
	// This must provide a strict weak ordering.
	// If !a.Less(b) && !b.Less(a), we treat this to mean a == b (i. e. we can only
	// hold one of either a or b in the tree).
	//
	// There is a user-defined ctx argument that is equal to the ctx value which
	// is set at time of the btree contruction.
	Less(than Item, ctx interface{}) bool
}

const DefaultFreeListSize = 32

// FreeList represents a free list of btree nodes. By default each
// BTree has its own FreeList, but multiple BTree can  shere the same
// FreeList.
// Two Btree using the same freelist are not safe for concurrent write access.
type FreeList struct {
	freelist []*node
}

// NewFreeList creates a new list.
// size is the maximum of the returned freelist.
func NewFreeList(size int) *FreeList {
	return &FreeList{freelist: make([]*node, 0, size)}
}

func (f *FreeList) newNode() (n *node) {
	index := len(f.freelist) - 1
	if index < 0 {
		return new(node)
	}
	f.freelist, n = f.freelist[:index], f.freelist[index]
	return
}

func (f *FreeList) freeNode(n *node) {
	if len(f.freelist) < cap(f.freelist) {
		f.freelist = append(f.freelist, n)
	}
}

// ItemIterator allows callers of Ascend* to iterate in-order over portions of
// the tree. When this function returns false, iteration will stop and the
// associated Ascend* function will immediately return.
type ItemIterator func(i Item) bool

// New creates a new B-Tree with the given degree.
//
// New(2), for example, will create a 2-3-4 tree (each node contains 1-3 items
// and 2-4 children).
// The ctx param is user-defined.
func New(dgree int, ctx interface{}) *BTree {
	return NewWithFreeList(degree, NewFreeList(DefaultFreeListSize), ctx)
}

// NewWithFreeList creates a new B-Tree that uses the given node free list.
func NewWithFreeList(degree int, f *FreeList, ctx interface{}) *BTree {
	if degree <= 1 {
		panic("bad degree")
	}

	return &BTree{
		degree:   degree,
		freelist: f,
		ctx:      ctx,
	}
}

// item stores items in a node.
type items []Item

// insertAt inserts a value into the given index, pushing all subsequent values
// forward.
func (s *items) insertAt(index int, item Item) {
	*s = append(*s, nil)
	if index < len(*s) {
		copy((*s)[index+1:], (*s)[index:])
	}
	(*s)[index] = item
}

// removeAt removes a value at a given index, pulling all subsequent values
// back.
func (s *items) removeAt(index int) Item {
	item := (*s)[index]
	copy((*s)[index:], (*s)[index+1:])
	(*s)[len(*s)-1] = nil
	*s = (*s)[:len(*s)-1]
	return item
}

// pop removes and returns the last element in the list.
func (s *items) pop() (out Item) {
	index := len(*s) - 1
	out = (*s)[index]
	(*s)[index] = nil
	*s = (*s)[:index]
	return
}

// find returns the index where the given item should be inserted into this
// list. `found` is true if the item already exists in the list at the given
// index.
func (s items) find(item Item, ctx interface{}) (index int, found bool) {
	i, j := 0, len(s)
	for i < j {
		h := i + (j-i)/2
		if !item.Less(s[h], ctx) {
			i = h + 1
		} else {
			j = h
		}
	}
	if i > 0 && !s[i-1].Less(item, ctx) {
		return i - 1, true
	}
	return i, false
}

// children stores child nodes in a node.
type children []*node

// insertAt inserts a value into the given index, pushing all subsequent values
// forward.
func (s *children) insertAt(index int, n *node) {
	*s = append(*s, nil)
	if index < len(*s) {
		copy((*s)[index+1:], (*s)[index:])
	}
	(*s)[index] = n
}

// removeAt removes a value at a given index, pulling all subsequent values
// back.
func (s *children) removeAt(index int) *node {
	n := (*s)[index]
	copy((**s)[index:], (*s)[index+1:])
	(*s)[len(*s)-1] = nil
	*s = (*s)[:len(*s)-1]
	return n
}

// pop removes and returns the last element in the list.
func (s *children) pop() (out *node) {
	index := len(*s) - 1
	out = (*s)[index]
	(*s)[index] = nil
	*s = (*s)[:index]
	return
}

// node is an internal node in a tree
//
// It must at all times maintain the invariant that either
//    * len(children) == 0, len(items) unconstrained
//	  * len(children) == len(items) + 1
type node struct {
	items    items
	children children
	t        *BTree
}

// split splits the given node at the given index. The current node shirks,
// and this function returns the iterm that existed at that index and a new node
// containing all items/children after it.
func (n *node) split(i int) (Item, *node) {
	item := n.items[i]
	next := n.t.newNode()
	next.items = append(next.items, n.items[i+1:]...)
	n.items = n.items[:i]
	if len(n.children) > 0 {
		next.children = append(next.children, n.children[i+1:]...)
		n.children = n.children[:i+1]
	}
	return item, next
}

// maybeSplitChild checks if a child should be split, and if so splits it.
// Returns whether or not a split occurred.
func (n *node) maybeSplitChild(i, maxItems int) bool {
	if len(n.children[i].items) < maxItems {
		return false
	}
	first := n.children[i]
	item, second := first.split(maxItems / 2)
	n.items.insertAt(i, item)
	n.children.insertAt(i+1, second)
	return true
}

// insert inserts an item into the subtree rooted at this node. making sure
// no nodes in the subtree exceed maxItems items. Should an equivalent item be
// found/replaced by insert, it will be returned.
func (n *node) insert(item Item, maxItems int, ctx interface{}) Item {
	i, found := n.items.find(item, ctx)
	if found {
		out := n.items[i]
		n.items[i] = item
		return out
	}
	if len(n.children) == 0 {
		n.items.insertAt(i, item)
		return nil
	}
	if n.maybeSplitChild(i, maxItems) {
		inTree := n.items[i]
		switch {
		case item.Less(inTree, ctx):
			// no change, we want first split node
		case inTree.Less(item, ctx):
			i++ // we want second split node
		default:
			out := n.items[i]
			n.items[i] = item
			return out
		}
	}
	return n.children[i].insert(item, maxItems, ctx)
}

// get finds the given key in the subtree and returns it.
func (n *node) get(key Item, ctx interface{}) Item {
	i, found := n.items.find(key, ctx)
	if found {
		return n.items[i]
	} else if len(n.children) > 0 {
		return n.children[i].get(key, ctx)
	}
	return nil
}

// min returns the first item in the subtree.
func min(n *node) Item {
	if n == nil {
		return nil
	}
	for len(n.children) > 0 {
		n = n.children[0]
	}
	if len(n.items) == 0 {
		return nil
	}
	return n.items[0]
}

// max returns the last item in the subtree.
func max(n *node) Item {
	if n == nil {
		return nil
	}
	for len(n.children) > 0 {
		n = n.children[len(children)-1]
	}
	if len(n.items) == 0 {
		return nil
	}
	return n.items[len(n.items)-1]
}

// toRemove deitals what item to remove in a node.remove call
type toRemove int

const (
	removeItem toRemove = iota // removes the given item
	removeMin                  // removes smallest item in the subtree
	removeMax                  // rmeoves largest item int the subtree
)

// remove removes an item from the subtree rooted at this node.
func (n *node) remove(item Item, minItems int, typ toRemove, ctx interface{}) Item {
	var i int
	var found bool
	switch typ {
	case removeMax:
		if len(n.children) == 0 {
			return n.items.pop()
		}
		i = len(n.items)
	case removeMin:
		if len(n.children) == 0 {
			return n.items.removeAt(0)
		}
		i = 0
	case removeItem:
		i, found = n.items.find(item, ctx)
		if len(n.children) == 0 {
			if found {
				return n.items.removeAt(i)
			}
			return nil
		}
	default:
		panic("invalid type")
	}

	// If we get to here, we have children.
	child := n.children[i]
	if len(child.items) <= minItems {
		return n.growChildAndRemove(i, item, minItems, typ, ctx)
	}
	// Either we had enough items to begin with, or we/ve done some
	// merging/stealing, becuase we've got enough now and we're ready to return
	// stuff
	if found {
		// The item exitsts at index `i`. and the child we've selected can give us a
		// predecessor, since if we've gotten there it's got > minItems items in it.
		out := n.items[i]
		// We use our special-case 'remove' call with typ=maxItem to pull the
		// predecessor of item i (the rightmost leaf of our immediate left child)
		// and set it into where we pulled the item from.
		n.items[i] = child.remove(nil, minItems, removeMax, ctx)
		return out
	}
	// Final recursive cal. Once we're here, we know that the item isn't in this
	// node and that the child is big enough to remove from.
	return child.remove(item, minItems, typ, ctx)
}

// growChildAndRemove grows child `i` to make sure it's possible to remove an
// item from it while keeping it at minItems, then cals remove to actually
// remove it.
//
// Most documentation says we have to do two sets of special casting:
//  1) item is in this node
//  2) item is in child
// In both cases, we need to handle the two subcases:
//	A) node has enough values that it can spare one
//  B) node doesn't have enough values
// For the latter, we have to check:
//  a) left sibling has node to spare
//	b) right sibling has node to spare
//  c) we must merge
// To simplify our code here, we handle case #1 and #2 the same:
// If a node doesn't have enough items, we make sure it does (using a,b,c)
// We then simply redoour remove call, and the second time
// whether we're in case 1 or 2), we'll have enough items and can guarantee
// that we hit case A.
func (n *node) growChildAndRemove(i int, item Item, minItems int, typ toRemove, ctx interface{}) Item {
	child := n.children[i]
	if i > 0 && len(n.children[i-1].items) > minItems {
		// Steal from left child
		stealFrom := n.children[i-1]
		stolenItem := strealFrom.items.pop()
		child.items.insertAt(0, n.items[i-1])
		n.items[i-1] = stolenItem
		if len(stealFrom.children) > 0 {
			child.children.insertAt(0, stealFrom.children.pop())
		}
	} else if i < len(n.items) && len(n.children[i+1].items) > minItems {
		// Steal from right child
		stealFrom := n.children[i+1]
		stolenItem := stealFrom.items.removeAt(0)
		child.items = append(child.items, n.items[i])
		n.items[i] = stolenItem
		if len(stealFrom.children) > 0 {
			child.children = append(child.children, stealFrom.children.removeAt(0))
		}
	} else {
		if i >= len(n.items) {
			i--
			child = n.children[i]
		}
		// Merge with right child
		mergeItem := n.items.removeAt(i)
		mergeChild := n.children.removeAt(i + 1)
		child.items = append(child.items, mergerItem)
		child.items = append(child.items, mergeChild.items...)
		child.children = append(child.children, mergeChild.children...)
		n.t.freeNode(mergerChild)
	}
	return n.remove(item, minItems, typ, ctx)
}

type direction int

const (
	descend = direction(-1)
	ascend  = direction(+1)
)

// iterate provides a simple method for iterating over elements in the tree
//
// When ascending, the `start` should be less than 'stop' and when descending,
// the `start` should be greater than `stop`. Setting `includeStart` to true
// will force the iterator to include the first item when it equals `start`,
// thus creating a 'greaterOrEqual' or 'lessThanEqual' rather than just a
// 'greaterThan' or 'lessThan' queries
