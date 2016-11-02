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
		panic("not equal")
	}
	for i, val := range p1.Pos {
		if p2.Pos[i] != val {
			panic("not equal")
		}
	}
	if string(p1.Data) != string(p2.Data) {
		panic("not equal")
	}
}

func TestPoint(t *testing.T) {
	var buf bytes.Buffer
	dims := rand.Intn(10) + 3
	maxData := rand.Intn(100) + 20
	var points [pointsToTest]Point
	for i := range points[:] {
		points[i] = NewPoint(dims, maxData)
		err := points[i].Serialize(&buf, maxData)
		if err != nil {
			panic(err)
		}
	}
	var points2 [pointsToTest]Point
	for i := range points2[:] {
		var err error
		points2[i], err = ParsePoint(&buf)
		if err != nil {
			panic(err)
		}
		AssertPointsEqual(points[i], points2[i])
	}
}
