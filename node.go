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
	"encoding/binary"
	"io"
)

type Node struct {
	Dim         uint32
	Left, Right int64
	Point       Point
}

func (n *Node) serialize(w io.Writer, maxDataLen int) error {
	err := n.Point.serialize(w, maxDataLen)
	if err != nil {
		return err
	}

	err = binary.Write(w, binary.LittleEndian, n.Left)
	if err != nil {
		return errClass.Wrap(err)
	}
	err = binary.Write(w, binary.LittleEndian, n.Right)
	if err != nil {
		return errClass.Wrap(err)
	}

	return errClass.Wrap(binary.Write(w, binary.LittleEndian, n.Dim))
}

func parseNode(data []byte) (rv Node, err error) {
	var remaining []byte
	rv.Point, remaining, err = parsePoint(data)
	if err != nil {
		return rv, err
	}
	rv.Left = int64(binary.LittleEndian.Uint64(remaining))
	remaining = remaining[uint64Size:]
	rv.Right = int64(binary.LittleEndian.Uint64(remaining))
	remaining = remaining[uint64Size:]
	rv.Dim = binary.LittleEndian.Uint32(remaining)
	remaining = remaining[uint32Size:]
	return rv, nil
}

func parseNodeFromReader(r io.Reader) (rv Node, maxDataLen int, err error) {
	rv.Point, maxDataLen, err = parsePointFromReader(r)
	if err != nil {
		return rv, 0, err
	}

	err = binary.Read(r, binary.LittleEndian, &rv.Left)
	if err != nil {
		return rv, 0, errClass.Wrap(err)
	}

	err = binary.Read(r, binary.LittleEndian, &rv.Right)
	if err != nil {
		return rv, 0, errClass.Wrap(err)
	}

	return rv, maxDataLen, errClass.Wrap(
		binary.Read(r, binary.LittleEndian, &rv.Dim))
}
