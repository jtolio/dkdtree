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
	"fmt"
	"math/rand"
	"testing"
)

func TestTree(t *testing.T) {
	iterations := 1
	points := 100
	searches := 10

	fs, err := newBaseFS(tempName("/tmp"))
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Delete()

	for i := 0; i < iterations; i++ {
		fmt.Printf("starting iteration %d\n", i+1)

		dims := rand.Intn(10) + 100
		maxData := rand.Intn(100) + 20

		log, err := NewPointSet(fs.Temp(), dims, maxData)
		if err != nil {
			t.Fatal(err)
		}
		defer log.Close()

		fmt.Printf("adding points (%d, %d)\n", dims, maxData)

		for i := 0; i < points; i++ {
			err = log.Add(NewPoint(dims, maxData))
			if err != nil {
				t.Fatal(err)
			}
		}

		fmt.Printf("creating tree\n")

		tree, err := CreateTree(fs.Path("tree"), fs.Temp(), log)
		if err != nil {
			t.Fatal(err)
		}

		tree2, err := OpenTree(fs.Path("tree"))
		if err != nil {
			t.Fatal(err)
		}

		for j := 0; j < searches; j++ {
			fmt.Printf("searching\n")

			q := NewPoint(dims, maxData)

			nearest, err := tree.Nearest(q, 10)
			if err != nil {
				t.Fatal(err)
			}

			nearest2, err := tree2.Nearest(q, 10)
			if err != nil {
				t.Fatal(err)
			}

			if len(nearest) != len(nearest2) {
				t.Fatal("uh oh")
			}

			last := float64(0)
			for i, resp := range nearest {
				if resp.Distance < last || !nearest2[i].Point.equal(&resp.Point) ||
					resp.Distance != nearest2[i].Distance {
					t.Fatal("uh oh")
				}
			}
		}
	}
}
