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

type node struct {
	Point       Point
	Dim         uint32
	Left, Right int64
}

func (n *node) Serialize(w io.Writer, maxDataLen int) error {
	err := n.Point.serialize(w, maxDataLen)
	if err != nil {
		return err
	}

	err = binary.Write(w, binary.BigEndian, n.Left)
	if err != nil {
		return Error.Wrap(err)
	}
	err = binary.Write(w, binary.BigEndian, n.Right)
	if err != nil {
		return Error.Wrap(err)
	}

	return Error.Wrap(binary.Write(w, binary.BigEndian, n.Dim))
}

func parseNode(r io.Reader) (rv node, err error) {
	rv.Point, err = parsePoint(r)
	if err != nil {
		return rv, err
	}

	err = binary.Read(r, binary.BigEndian, &rv.Left)
	if err != nil {
		return rv, Error.Wrap(err)
	}

	err = binary.Read(r, binary.BigEndian, &rv.Right)
	if err != nil {
		return rv, Error.Wrap(err)
	}

	return rv, Error.Wrap(binary.Read(r, binary.BigEndian, &rv.Dim))
}
