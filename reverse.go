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
	"io"
	"os"
)

func reverseTree(oldpath, newpath string) error {
	fh, err := os.Open(oldpath)
	if err != nil {
		return err
	}
	defer fh.Close()

	filelen, err := fh.Seek(0, 2)
	if err != nil {
		return err
	}
	if filelen == 0 {
		dest, err := os.Create(newpath)
		if err != nil {
			return err
		}
		dest.Close()
		return nil
	}

	_, err = fh.Seek(0, 0)
	if err != nil {
		return err
	}

	source := &wrappedReader{r: bufio.NewReader(fh)}
	dest, err := os.Create(newpath)
	if err != nil {
		return err
	}
	defer dest.Close()

	nodelen := int64(-1)
	maxDataLen := -1
	for node_idx := int64(0); true; node_idx++ {
		node, nodeMaxDataLen, err := parseNodeFromReader(source)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if maxDataLen == -1 {
			maxDataLen = nodeMaxDataLen
		}
		if nodeMaxDataLen != maxDataLen {
			return errClass.New("disparate max data len")
		}
		if nodelen == -1 {
			nodelen = source.pos
			if filelen%nodelen != 0 {
				return errClass.New("Invalid tree file")
			}
		}
		_, err = dest.Seek(filelen-nodelen*(1+node_idx), 0)
		if err != nil {
			return err
		}

		if node.Left != -1 {
			node.Left = filelen - nodelen - node.Left
		}
		if node.Right != -1 {
			node.Right = filelen - nodelen - node.Right
		}

		err = node.serialize(dest, maxDataLen)
		if err != nil {
			return err
		}
	}

	return nil
}
