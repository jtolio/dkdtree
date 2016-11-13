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
	"io"
)

type writeMeter struct {
	w      io.Writer
	Amount int64
}

func newWriteMeter(w io.Writer) *writeMeter {
	return &writeMeter{w: w}
}

func (w *writeMeter) Write(p []byte) (n int, err error) {
	n, err = w.w.Write(p)
	w.Amount += int64(n)
	return n, err
}

type wrappedReader struct {
	r   io.Reader
	pos int64
}

func (w *wrappedReader) Read(p []byte) (n int, err error) {
	n, err = w.r.Read(p)
	w.pos += int64(n)
	return n, err
}
