package dkdtree

import (
	"bufio"
	"os"

	"github.com/spacemonkeygo/errors"
)

type NodeLog struct {
	fh               *os.File
	buf              *bufio.Writer
	dims, maxDataLen int
	offset           int64
}

func NewNodeLog(path string, dims, maxDataLen int) (*NodeLog, error) {
	fh, err := os.Create(path)
	if err != nil {
		return nil, Error.Wrap(err)
	}
	return &NodeLog{
		fh:         fh,
		buf:        bufio.NewWriter(fh),
		dims:       dims,
		maxDataLen: maxDataLen,
	}, nil
}

func (nl *NodeLog) Close() error {
	var errs errors.ErrorGroup
	errs.Add(nl.buf.Flush())
	errs.Add(nl.fh.Close())
	return errs.Finalize()
}

func (nl *NodeLog) Add(n Node) (offset int64, err error) {
	offset = nl.offset

	if len(n.Point.Pos) != nl.dims {
		return offset, Error.New("point has wrong dimension: %d, expected %d",
			len(n.Point.Pos), nl.dims)
	}

	meter := NewWriteMeter(nl.buf)
	err = n.Serialize(meter, nl.maxDataLen)
	nl.offset += meter.Amount
	return offset, err
}

func (nl *NodeLog) Build(fs *FS, log *PointLog, dim int) (node_offset int64,
	err error) {
	defer log.Close()
	if log.Len() == 0 {
		return -1, nil
	}

	median := log.MedianEstimate(dim)
	left, right, err := log.Split(fs, median, dim, true)
	if err != nil {
		return -1, err
	}

	defer left.Close()
	defer right.Close()

	ndim := (dim + 1) % log.Dims()

	leftOffset, err := nl.Build(fs, left, ndim)
	if err != nil {
		return -1, err
	}

	rightOffset, err := nl.Build(fs, right, ndim)
	if err != nil {
		return -1, err
	}

	return nl.Add(Node{
		Point: median,
		Dim:   uint32(dim),
		Left:  leftOffset,
		Right: rightOffset})
}
