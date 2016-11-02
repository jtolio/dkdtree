package dkdtree

import (
	"bufio"
	"math/rand"
	"os"
	"sort"

	"github.com/spacemonkeygo/errors"
)

const (
	samplingSize = 100
)

type PointLog struct {
	fh               *os.File
	buf              *bufio.Writer
	dims, maxDataLen int
	count            int64
	reservoir        []Point
	deleteOnClose    bool
	deleted          bool
	path             string
}

func NewPointLog(path string, dims, maxDataLen int, deleteOnClose bool) (
	*PointLog, error) {
	fh, err := os.Create(path)
	if err != nil {
		return nil, Error.Wrap(err)
	}
	return &PointLog{
		fh:            fh,
		buf:           bufio.NewWriter(fh),
		dims:          dims,
		maxDataLen:    maxDataLen,
		reservoir:     make([]Point, 0, samplingSize),
		deleteOnClose: deleteOnClose,
		path:          path,
	}, nil
}

func (pl *PointLog) closeNoDel() error {
	var errs errors.ErrorGroup
	if pl.buf != nil {
		errs.Add(pl.buf.Flush())
		pl.buf = nil
	}
	if pl.fh != nil {
		errs.Add(pl.fh.Close())
		pl.fh = nil
	}
	pl.reservoir = nil
	return errs.Finalize()
}

func (pl *PointLog) del() error {
	if !pl.deleted {
		pl.deleted = true
		return os.Remove(pl.path)
	}
	return nil
}

func (pl *PointLog) Close() error {
	var errs errors.ErrorGroup
	errs.Add(pl.closeNoDel())
	if pl.deleteOnClose {
		errs.Add(pl.del())
	}
	return errs.Finalize()
}

func (pl *PointLog) Len() int64      { return pl.count }
func (pl *PointLog) MaxDataLen() int { return pl.maxDataLen }
func (pl *PointLog) Dims() int       { return pl.dims }

func (pl *PointLog) Add(p Point) error {
	if len(p.Pos) != pl.dims {
		return Error.New("point has wrong dimension: %d, expected %d",
			len(p.Pos), pl.dims)
	}
	err := p.Serialize(pl.buf, pl.maxDataLen)
	if err != nil {
		return err
	}
	pl.count += 1
	if len(pl.reservoir) < cap(pl.reservoir) {
		pl.reservoir = append(pl.reservoir, p)
	} else {
		pos := rand.Int63n(pl.count)
		if pos < int64(len(pl.reservoir)) {
			pl.reservoir[pos] = p
		}
	}
	return nil
}

func (pl *PointLog) Split(fs *FS, median Point, dim int, deleteOnClose bool) (
	left, right *PointLog, err error) {
	defer pl.Close()
	err = pl.closeNoDel()
	if err != nil {
		return nil, nil, err
	}

	fh, err := os.Open(pl.path)
	if err != nil {
		return nil, nil, err
	}
	defer fh.Close()

	fhbuf := bufio.NewReader(fh)

	left, err = NewPointLog(fs.Temp(), pl.dims, pl.maxDataLen, deleteOnClose)
	if err != nil {
		return nil, nil, err
	}

	right, err = NewPointLog(fs.Temp(), pl.dims, pl.maxDataLen, deleteOnClose)
	if err != nil {
		left.closeNoDel()
		left.del()
		return nil, nil, err
	}

	closeUp := func() {
		left.closeNoDel()
		left.del()
		right.closeNoDel()
		right.del()
	}

	foundMedian := false
	for i := int64(0); i < pl.count; i++ {
		p, err := ParsePoint(fhbuf)
		if err != nil {
			closeUp()
			return nil, nil, err
		}
		if !foundMedian && median.Equal(&p) {
			foundMedian = true
			continue
		}
		if p.Pos[dim] <= median.Pos[dim] {
			err = left.Add(p)
		} else {
			err = right.Add(p)
		}
		if err != nil {
			closeUp()
			return nil, nil, err
		}
	}

	return left, right, nil
}

func (pl *PointLog) MedianEstimate(dim int) Point {
	if len(pl.reservoir) == 0 {
		panic("no points in reservoir")
	}
	ps := PointSorter{
		Dim:    dim,
		Points: append([]Point(nil), pl.reservoir...)}
	sort.Sort(&ps)
	return ps.Points[len(ps.Points)/2]
}

type PointSorter struct {
	Dim    int
	Points []Point
}

func (p *PointSorter) Len() int { return len(p.Points) }
func (p *PointSorter) Less(i, j int) bool {
	return p.Points[i].Pos[p.Dim] < p.Points[j].Pos[p.Dim]
}
func (p *PointSorter) Swap(i, j int) {
	p.Points[i], p.Points[j] = p.Points[j], p.Points[i]
}
