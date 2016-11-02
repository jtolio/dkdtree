package dkdtree

import (
	"bytes"
	"encoding/binary"
	"io"
	"io/ioutil"
)

const (
	// these assumptions are coded into serialization version 0
	float64Size = 8
	uint32Size  = 4
)

func init() {
	if float64Size != binary.Size(float64(0)) ||
		uint32Size != binary.Size(uint32(0)) {
		panic("uh oh")
	}
}

type Point struct {
	Pos  []float64
	Data []byte
}

func (p1 *Point) Equal(p2 *Point) bool {
	if len(p1.Pos) != len(p2.Pos) ||
		len(p1.Data) != len(p2.Data) {
		return false
	}
	for i, f1 := range p1.Pos {
		if p2.Pos[i] != f1 {
			return false
		}
	}
	return bytes.Equal(p1.Data, p2.Data)
}

func (p1 *Point) DistanceSquared(p2 *Point) (sum float64) {
	for i, v := range p1.Pos {
		delta := v - p2.Pos[i]
		sum += delta * delta
	}
	return sum
}

func (p *Point) Serialize(w io.Writer, maxDataLen int) error {
	if len(p.Data) > maxDataLen {
		return Error.New("data length (%d) greater than max data length (%d)",
			len(p.Data), maxDataLen)
	}
	// serialization version
	_, err := w.Write([]byte{0})
	if err != nil {
		return Error.Wrap(err)
	}
	// number of floating point values
	posLen := uint32(len(p.Pos))
	err = binary.Write(w, binary.BigEndian, posLen)
	if err != nil {
		return Error.Wrap(err)
	}
	// number of data bytes
	dataLen := uint32(len(p.Data))
	err = binary.Write(w, binary.BigEndian, dataLen)
	if err != nil {
		return Error.Wrap(err)
	}
	// padding
	paddingLen := uint32(maxDataLen - len(p.Data))
	err = binary.Write(w, binary.BigEndian, paddingLen)
	if err != nil {
		return Error.Wrap(err)
	}
	// floating point values
	for _, val := range p.Pos {
		err = binary.Write(w, binary.BigEndian, val)
		if err != nil {
			return Error.Wrap(err)
		}
	}
	// data
	_, err = w.Write(p.Data)
	if err != nil {
		return Error.Wrap(err)
	}
	// padding
	_, err = w.Write(make([]byte, paddingLen))
	return Error.Wrap(err)
}

func ParsePoint(r io.Reader) (rv Point, err error) {
	var version [1]byte
	_, err = io.ReadFull(r, version[:])
	if err != nil {
		return rv, Error.Wrap(err)
	}
	if version[0] != 0 {
		return rv, Error.New("invalid serialization version")
	}

	// pos, data, padding
	var lens [3]uint32
	for i := range lens[:] {
		err = binary.Read(r, binary.BigEndian, &(lens[i]))
		if err != nil {
			return rv, Error.Wrap(err)
		}
	}

	rv.Pos = make([]float64, lens[0])
	rv.Data = make([]byte, lens[1])

	for i := range rv.Pos {
		err = binary.Read(r, binary.BigEndian, &(rv.Pos[i]))
		if err != nil {
			return rv, Error.Wrap(err)
		}
	}

	_, err = io.ReadFull(r, rv.Data)
	if err != nil {
		return rv, Error.Wrap(err)
	}

	_, err = io.CopyN(ioutil.Discard, r, int64(lens[2]))
	return rv, Error.Wrap(err)
}
