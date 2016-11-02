package dkdtree

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestTree(t *testing.T) {
	iterations := 1
	points := 1000
	searches := 10

	fs, err := NewFS(tempName("/tmp"))
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Delete()

	for i := 0; i < iterations; i++ {
		fmt.Printf("starting iteration %d\n", i+1)

		dims := rand.Intn(10) + 1000
		maxData := rand.Intn(100) + 20

		log, err := NewPointLog(fs.Temp(), dims, maxData, true)
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

		tree, err := CreateTree(fs, fs.Path("tree"), log)
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
				if resp.Distance < last || !nearest2[i].Point.Equal(&resp.Point) ||
					resp.Distance != nearest2[i].Distance {
					t.Fatal("uh oh")
				}
			}
		}
	}
}
