package dkdtree

import (
	"bufio"
	"container/heap"
	"fmt"
	"os"
	"sort"

	"github.com/spacemonkeygo/errors"
)

var (
	Error = errors.NewClass("dkdtree")
)

type Tree struct {
	path  string
	fh    *os.File
	Root  int64
	Count int64
}

func CreateTree(fs *FS, path string, log *PointLog) (*Tree, error) {
	count := log.Len()

	nlog, err := NewNodeLog(path, log.Dims(), log.MaxDataLen())
	if err != nil {
		return nil, err
	}

	root_offset, err := nlog.Build(fs, log, 0)
	if err != nil {
		return nil, err
	}

	nlog.Close()
	if err != nil {
		return nil, err
	}

	fh, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return &Tree{
		path:  path,
		fh:    fh,
		Root:  root_offset,
		Count: count,
	}, nil
}

func OpenTree(path string) (*Tree, error) {
	fh, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	filelen, err := fh.Seek(0, 2)
	if err != nil {
		fh.Close()
		return nil, err
	}
	if filelen == 0 {
		return &Tree{path: path, fh: fh, Root: -1, Count: 0}, nil
	}

	_, err = fh.Seek(0, 0)
	if err != nil {
		fh.Close()
		return nil, err
	}

	_, err = ParseNode(fh)
	if err != nil {
		fh.Close()
		return nil, err
	}

	nodelen, err := fh.Seek(0, 1)
	if err != nil {
		fh.Close()
		return nil, err
	}

	if filelen%nodelen != 0 {
		fh.Close()
		return nil, fmt.Errorf("Invalid tree file")
	}

	return &Tree{
		path:  path,
		fh:    fh,
		Root:  filelen - nodelen,
		Count: filelen / nodelen,
	}, nil
}

func (t *Tree) node(offset int64) (Node, error) {
	_, err := t.fh.Seek(offset, 0)
	if err != nil {
		return Node{}, err
	}
	return ParseNode(bufio.NewReader(t.fh))
}

func (t *Tree) Close() error {
	return t.fh.Close()
}

type PointDistance struct {
	Point
	Distance float64
}

type maxHeap []PointDistance

func (h *maxHeap) Max() PointDistance { return (*h)[0] }
func (h *maxHeap) Len() int           { return len(*h) }
func (h *maxHeap) Cap() int           { return cap(*h) }

func (h *maxHeap) Less(i, j int) bool {
	return (*h)[i].Distance > (*h)[j].Distance
}

func (h *maxHeap) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
}

func (h *maxHeap) Push(x interface{}) {
	(*h) = append(*h, x.(PointDistance))
}

func (h *maxHeap) Pop() (i interface{}) {
	i, *h = (*h)[len(*h)-1], (*h)[:len(*h)-1]
	return i
}

func (t *Tree) Nearest(p Point, n int) ([]PointDistance, error) {
	h := make(maxHeap, 0, n)
	err := t.search(t.Root, p, &h)
	if err != nil {
		return nil, err
	}
	sort.Sort(sort.Reverse(&h))
	return h, nil
}

func (t *Tree) search(node_offset int64, p Point, h *maxHeap) error {
	if node_offset == -1 {
		return nil
	}

	node, err := t.node(node_offset)
	if err != nil {
		return err
	}

	c := p.Pos[node.Dim] - node.Point.Pos[node.Dim]
	dist := p.DistanceSquared(&node.Point)

	if h.Len() < h.Cap() || dist < h.Max().Distance {
		for h.Len() >= h.Cap() {
			heap.Pop(h)
		}
		heap.Push(h, PointDistance{
			Point:    node.Point,
			Distance: dist})
	}

	if c <= 0 {
		err = t.search(node.Left, p, h)
		if err != nil {
			return err
		}
		if c*c <= h.Max().Distance {
			err = t.search(node.Right, p, h)
			if err != nil {
				return err
			}
		}
		return nil
	}

	err = t.search(node.Right, p, h)
	if err != nil {
		return err
	}
	if c*c <= h.Max().Distance {
		err = t.search(node.Left, p, h)
		if err != nil {
			return err
		}
	}
	return nil
}
