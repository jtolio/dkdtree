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
	"bytes"
	crand "crypto/rand"
	"math/rand"
	"testing"
)

const (
	pointsToTest = 20
)

func NewPoint(dims, maxData int) (rv Point) {
	rv.Pos = make([]float64, 0, dims)
	for i := 0; i < dims; i++ {
		rv.Pos = append(rv.Pos, rand.Float64())
	}
	rv.Data = make([]byte, rand.Intn(maxData))
	_, err := crand.Read(rv.Data)
	if err != nil {
		panic(err)
	}
	return rv
}

func AssertPointsEqual(p1, p2 Point) {
	if len(p1.Pos) != len(p2.Pos) {
		if p1.equal(&p2) {
			panic("uh oh, equal is wrong")
		}
		panic("not equal")
	}
	for i, val := range p1.Pos {
		if p2.Pos[i] != val {
			if p1.equal(&p2) {
				panic("uh oh, equal is wrong")
			}
			panic("not equal")
		}
	}
	if string(p1.Data) != string(p2.Data) {
		if p1.equal(&p2) {
			panic("uh oh, equal is wrong")
		}
		panic("not equal")
	}
	if !p1.equal(&p2) {
		panic("uh oh, equal is wrong")
	}
}

func TestPoint(t *testing.T) {
	var buf bytes.Buffer
	dims := rand.Intn(10) + 3
	maxData := rand.Intn(100) + 20
	var points [pointsToTest]Point
	for i := range points[:] {
		points[i] = NewPoint(dims, maxData)
		err := points[i].serialize(&buf, maxData)
		if err != nil {
			panic(err)
		}
	}
	var points2 [pointsToTest]Point
	for i := range points2[:] {
		tp, _, err := parsePoint(buf.Bytes())
		if err != nil {
			panic(err)
		}
		points2[i], _, err = parsePointFromReader(&buf)
		if err != nil {
			panic(err)
		}
		AssertPointsEqual(points[i], points2[i])
		AssertPointsEqual(points[i], tp)
	}
}
