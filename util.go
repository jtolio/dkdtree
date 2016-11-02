package dkdtree

import (
	"io"
)

type WriteMeter struct {
	w      io.Writer
	Amount int64
}

func NewWriteMeter(w io.Writer) *WriteMeter {
	return &WriteMeter{w: w}
}

func (w *WriteMeter) Write(p []byte) (n int, err error) {
	n, err = w.w.Write(p)
	w.Amount += int64(n)
	return n, err
}
