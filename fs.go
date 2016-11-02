package dkdtree

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
)

type FS struct {
	base string
}

func NewFS(path string) (*FS, error) {
	err := os.MkdirAll(path, 0777)
	if err != nil {
		return nil, Error.New("unable to open %#v: %v", path, err)
	}
	err = os.MkdirAll(filepath.Join(path, "tmp"), 0777)
	if err != nil {
		return nil, Error.New("unable to open %#v: %v", path, err)
	}
	err = os.MkdirAll(filepath.Join(path, "named"), 0777)
	if err != nil {
		return nil, Error.New("unable to open %#v: %v", path, err)
	}
	return &FS{
		base: path,
	}, nil
}

func (fs *FS) Path(name ...string) string {
	return filepath.Join(append([]string{fs.base, "named"}, name...)...)
}

func tempName(base string) string {
	for {
		var buf [16]byte
		_, err := rand.Read(buf[:])
		if err != nil {
			panic(err)
		}
		path := filepath.Join(base, hex.EncodeToString(buf[:]))
		_, err = os.Stat(path)
		if err == nil {
			continue
		}
		if os.IsNotExist(err) {
			return path
		}
		panic(err)
	}

}

func (fs *FS) Temp() string {
	return tempName(filepath.Join(fs.base, "tmp"))
}

func (fs *FS) Delete() error {
	return os.RemoveAll(fs.base)
}
