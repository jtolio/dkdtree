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
	"os"

	"github.com/spacemonkeygo/errors"
)

type nodeLog struct {
	fh               *os.File
	buf              *bufio.Writer
	dims, maxDataLen int
	offset           int64
}

func newNodeLog(path string, dims, maxDataLen int) (*nodeLog, error) {
	fh, err := os.Create(path)
	if err != nil {
		return nil, Error.Wrap(err)
	}
	return &nodeLog{
		fh:         fh,
		buf:        bufio.NewWriter(fh),
		dims:       dims,
		maxDataLen: maxDataLen,
	}, nil
}

func (nl *nodeLog) Close() error {
	var errs errors.ErrorGroup
	errs.Add(nl.buf.Flush())
	errs.Add(nl.fh.Close())
	return errs.Finalize()
}

func (nl *nodeLog) Add(n node) (offset int64, err error) {
	offset = nl.offset

	if len(n.Point.Pos) != nl.dims {
		return offset, Error.New("point has wrong dimension: %d, expected %d",
			len(n.Point.Pos), nl.dims)
	}

	meter := newWriteMeter(nl.buf)
	err = n.Serialize(meter, nl.maxDataLen)
	nl.offset += meter.Amount
	return offset, err
}

func (nl *nodeLog) Build(fs *baseFS, log *PointSet, dim int) (
	node_offset int64, err error) {
	defer log.Close()
	if log.count == 0 {
		return -1, nil
	}

	median := log.medianEstimate(dim)
	left, right, err := log.split(fs, median, dim, true)
	if err != nil {
		return -1, err
	}

	defer left.Close()
	defer right.Close()

	ndim := (dim + 1) % log.dims

	leftOffset, err := nl.Build(fs, left, ndim)
	if err != nil {
		return -1, err
	}

	rightOffset, err := nl.Build(fs, right, ndim)
	if err != nil {
		return -1, err
	}

	return nl.Add(node{
		Point: median,
		Dim:   uint32(dim),
		Left:  leftOffset,
		Right: rightOffset})
}
