package s3fs

import (
	"sync"

	"github.com/eikenb/pipeat"
)

var _ WriterCloserAt = (*writer)(nil)

type writer struct {
	w    *pipeat.PipeWriterAt
	err  error
	once sync.Once
}

func newWriter(w *pipeat.PipeWriterAt) *writer {
	return &writer{
		w: w,
	}
}

func (w *writer) Close() error {
	return w.CloseWithError(nil)
}

func (w *writer) CloseWithError(err error) error {
	w.once.Do(func() {
		w.err = w.w.CloseWithError(err)
	})
	return w.err
}

func (w *writer) Write(p []byte) (n int, err error) {
	return w.w.Write(p)
}

func (w *writer) WriteAt(p []byte, off int64) (n int, err error) {
	return w.w.WriteAt(p, off)
}
