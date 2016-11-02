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
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
)

type baseFS struct {
	base string
}

func newBaseFS(path string) (*baseFS, error) {
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
	return &baseFS{
		base: path,
	}, nil
}

func (fs *baseFS) Path(name ...string) string {
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

func (fs *baseFS) Temp() string {
	return tempName(filepath.Join(fs.base, "tmp"))
}

func (fs *baseFS) Delete() error {
	return os.RemoveAll(fs.base)
}
