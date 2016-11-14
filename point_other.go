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

// +build !amd64

package dkdtree

import (
	"encoding/binary"
	"io"
)

func readFloats(r io.Reader, amount uint32) ([]float64, error) {
	rv := make([]float64, amount)
	return rv, binary.Read(r, binary.LittleEndian, rv)
}
