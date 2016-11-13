// Copyright (C) 2016 JT Olds
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package dkdtree implements a disk-backed kd-tree for datasets too large for
// memory.
package dkdtree

import (
	"bytes"
	"container/heap"
	"io"
	"os"
	"sort"

	"github.com/spacemonkeygo/errors"
)

var (
	errClass = errors.NewClass("dkdtree")
)

type Tree struct {
	path    string
	fh      *os.File
	root    int64
	count   int64
	nodelen int64
}

func CreateTree(path, tmpdir string, points *PointSet) (*Tree, error) {
	fs, err := newBaseFS(tempName(tmpdir))
	if err != nil {
		return nil, err
	}
	defer fs.Delete()

	reversed := fs.Temp()

	nlog, err := newNodeLog(reversed, points.dims, points.maxDataLen)
	if err != nil {
		return nil, err
	}

	_, err = nlog.Build(fs, points, 0)
	if err != nil {
		return nil, err
	}

	err = nlog.Close()
	if err != nil {
		return nil, err
	}

	err = reverseTree(reversed, path)
	if err != nil {
		return nil, err
	}

	return OpenTree(path)
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
		return &Tree{path: path, fh: fh, root: -1, count: 0}, nil
	}

	_, err = fh.Seek(0, 0)
	if err != nil {
		fh.Close()
		return nil, err
	}

	_, _, err = parseNode(fh)
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
		return nil, errClass.New("Invalid tree file")
	}

	return &Tree{
		path:    path,
		fh:      fh,
		root:    0,
		count:   filelen / nodelen,
		nodelen: nodelen,
	}, nil
}

func (t *Tree) Close() error {
	return t.fh.Close()
}

func (t *Tree) Count() int64        { return t.count }
func (t *Tree) Root() (Node, error) { return t.Node(t.root) }

func (t *Tree) Node(id int64) (Node, error) {
	_, err := t.fh.Seek(id, 0)
	if err != nil {
		return Node{}, err
	}
	data := make([]byte, t.nodelen)
	_, err = io.ReadFull(t.fh, data)
	if err != nil {
		return Node{}, err
	}
	n, _, err := parseNode(bytes.NewReader(data))
	return n, err
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
	err := t.search(t.root, p, &h)
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

	n, err := t.Node(node_offset)
	if err != nil {
		return err
	}

	c := p.Pos[n.Dim] - n.Point.Pos[n.Dim]
	dist := p.distanceSquared(&n.Point)

	if h.Len() < h.Cap() || dist < h.Max().Distance {
		for h.Len() >= h.Cap() {
			heap.Pop(h)
		}
		heap.Push(h, PointDistance{
			Point:    n.Point,
			Distance: dist})
	}

	if c <= 0 {
		err = t.search(n.Left, p, h)
		if err != nil {
			return err
		}
		if c*c <= h.Max().Distance {
			err = t.search(n.Right, p, h)
			if err != nil {
				return err
			}
		}
		return nil
	}

	err = t.search(n.Right, p, h)
	if err != nil {
		return err
	}
	if c*c <= h.Max().Distance {
		err = t.search(n.Left, p, h)
		if err != nil {
			return err
		}
	}
	return nil
}
