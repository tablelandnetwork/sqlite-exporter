package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/DataDog/zstd"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// Decompress decompresses a zstd file.
func Decompress(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open file: %s", err)
	}
	pr, pw := io.Pipe()
	gzR := zstd.NewReader(file)

	errs := errgroup.Group{}
	errs.Go(func() error {
		if _, err := io.Copy(pw, gzR); err != nil {
			return errors.Errorf("copy to writer: %s", err)
		}

		if err := gzR.Close(); err != nil {
			return errors.Errorf("closing reader: %s", err)
		}
		if err := pw.Close(); err != nil {
			return errors.Errorf("closing pipe writer: %s", err)
		}

		return nil
	})

	newFilepath := strings.TrimSuffix(path, "."+filepath.Ext(path))
	df, err := os.OpenFile(newFilepath, os.O_CREATE|os.O_WRONLY, 0o755)
	if err != nil {
		return "", errors.Errorf("open new file: %s", err)
	}

	writer := bufio.NewWriter(df)
	if _, err := io.Copy(writer, pr); err != nil {
		return "", errors.Errorf("copy dest file: %s", err)
	}

	if err := errs.Wait(); err != nil {
		return "", errors.Errorf("errgroup wait: %s", err)
	}
	return newFilepath, nil
}
