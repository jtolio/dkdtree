package dkdtree

import (
	"encoding/binary"
	"io"
)

type Node struct {
	Point       Point
	Dim         uint32
	Left, Right int64
}

func (n *Node) Serialize(w io.Writer, maxDataLen int) error {
	err := n.Point.Serialize(w, maxDataLen)
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

func ParseNode(r io.Reader) (rv Node, err error) {
	rv.Point, err = ParsePoint(r)
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
