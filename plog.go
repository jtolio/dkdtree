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

package dkdtree

import (
	"bufio"
	"math/rand"
	"os"
	"sort"

	"github.com/spacemonkeygo/errors"
)

const (
	samplingSize = 100
)

type PointSet struct {
	fh               *os.File
	buf              *bufio.Writer
	dims, maxDataLen int
	count            int64
	reservoir        []Point
	deleteOnClose    bool
	deleted          bool
	path             string
}

func newPointSet(path string, dims, maxDataLen int, deleteOnClose bool) (
	*PointSet, error) {
	fh, err := os.Create(path)
	if err != nil {
		return nil, errClass.Wrap(err)
	}
	return &PointSet{
		fh:            fh,
		buf:           bufio.NewWriter(fh),
		dims:          dims,
		maxDataLen:    maxDataLen,
		reservoir:     make([]Point, 0, samplingSize),
		deleteOnClose: deleteOnClose,
		path:          path,
	}, nil
}

func NewPointSet(path string, dims, maxDataLen int) (*PointSet, error) {
	return newPointSet(path, dims, maxDataLen, false)
}

func (pl *PointSet) closeNoDel() error {
	var errs errors.ErrorGroup
	if pl.buf != nil {
		errs.Add(pl.buf.Flush())
		pl.buf = nil
	}
	if pl.fh != nil {
		errs.Add(pl.fh.Close())
		pl.fh = nil
	}
	pl.reservoir = nil
	return errs.Finalize()
}

func (pl *PointSet) del() error {
	if !pl.deleted {
		pl.deleted = true
		return os.Remove(pl.path)
	}
	return nil
}

func (pl *PointSet) Close() error {
	var errs errors.ErrorGroup
	errs.Add(pl.closeNoDel())
	if pl.deleteOnClose {
		errs.Add(pl.del())
	}
	return errs.Finalize()
}

func (pl *PointSet) Add(p Point) error {
	if len(p.Pos) != pl.dims {
		return errClass.New("point has wrong dimension: %d, expected %d",
			len(p.Pos), pl.dims)
	}
	err := p.serialize(pl.buf, pl.maxDataLen)
	if err != nil {
		return err
	}
	pl.count += 1
	if len(pl.reservoir) < cap(pl.reservoir) {
		pl.reservoir = append(pl.reservoir, p)
	} else {
		pos := rand.Int63n(pl.count)
		if pos < int64(len(pl.reservoir)) {
			pl.reservoir[pos] = p
		}
	}
	return nil
}

func (pl *PointSet) split(fs *baseFS, median Point, dim int,
	deleteOnClose bool) (left, right *PointSet, err error) {
	defer pl.Close()
	err = pl.closeNoDel()
	if err != nil {
		return nil, nil, err
	}

	fh, err := os.Open(pl.path)
	if err != nil {
		return nil, nil, err
	}
	defer fh.Close()

	fhbuf := bufio.NewReader(fh)

	left, err = newPointSet(fs.Temp(), pl.dims, pl.maxDataLen, deleteOnClose)
	if err != nil {
		return nil, nil, err
	}

	right, err = newPointSet(fs.Temp(), pl.dims, pl.maxDataLen, deleteOnClose)
	if err != nil {
		left.closeNoDel()
		left.del()
		return nil, nil, err
	}

	closeUp := func() {
		left.closeNoDel()
		left.del()
		right.closeNoDel()
		right.del()
	}

	foundMedian := false
	for i := int64(0); i < pl.count; i++ {
		p, _, err := parsePoint(fhbuf)
		if err != nil {
			closeUp()
			return nil, nil, err
		}
		if !foundMedian && median.equal(&p) {
			foundMedian = true
			continue
		}
		if p.Pos[dim] <= median.Pos[dim] {
			err = left.Add(p)
		} else {
			err = right.Add(p)
		}
		if err != nil {
			closeUp()
			return nil, nil, err
		}
	}

	return left, right, nil
}

func (pl *PointSet) medianEstimate(dim int) Point {
	if len(pl.reservoir) == 0 {
		panic("no points in reservoir")
	}
	ps := pointSorter{
		Dim:    dim,
		Points: append([]Point(nil), pl.reservoir...)}
	sort.Sort(&ps)
	return ps.Points[len(ps.Points)/2]
}

type pointSorter struct {
	Dim    int
	Points []Point
}

func (p *pointSorter) Len() int { return len(p.Points) }
func (p *pointSorter) Less(i, j int) bool {
	return p.Points[i].Pos[p.Dim] < p.Points[j].Pos[p.Dim]
}
func (p *pointSorter) Swap(i, j int) {
	p.Points[i], p.Points[j] = p.Points[j], p.Points[i]
}
