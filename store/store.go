package store

import (
	"encoding/binary"
	"fmt"
	"github.com/travisjeffery/jocko/commitlog"
	"io"
)
type Store struct {
	source *commitlog.CommitLog
	reader io.Reader
}
func OpenStore(path string) (Store, error) {
	log, err := commitlog.New(commitlog.Options{
		Path:            path,
		MaxSegmentBytes: 1024,
		MaxLogBytes:     -1,
		CleanupPolicy:   commitlog.CompactCleanupPolicy,
	})
	if err != nil {
		return Store{}, err
	}
	reader, err := log.NewReader(0,0)
	if err != nil {
		return Store{}, err
	}
	return Store{log, reader}, nil
}

func (s Store) Write(key []byte, value []byte) error{
	_, err := s.source.Append(newMessageSet(newMessage(key, value)))
	if err != nil {
		return err
	}
	return nil
}

func (s Store) Reader(offset int64) (StoreReader, error) {
	r, err := s.source.NewReader(offset, 0)
	if err != nil {
		return StoreReader{}, err
	}
	return StoreReader{reader:r}, nil

}

func (s Store) Close() error{
	return s.source.Close()
}

type StoreReader struct {
	reader io.Reader
}

func (sr StoreReader) Read() (key string, value []byte, err error) {
	setBuf := make([]byte, offsetSize+messageSizeSize)
	n, err := sr.reader.Read(setBuf)
	if err != io.EOF && err != nil {
		return "", nil, err
	}
	if n != offsetSize+messageSizeSize {
		return "", nil, err
	}
	size := binary.BigEndian.Uint32(setBuf[messageSizeStart:])
	msgBuf := make([]byte, size)
	n, err = sr.reader.Read(msgBuf)
	if err != nil {
		return "", nil, err
	}
	if n != int(size) {
		return "", nil, fmt.Errorf("Size %d did not match %d read bytes", size, n)
	}
	buf := make([]byte, len(setBuf)+len(msgBuf))
	copy(buf[:messageSizeEnd], setBuf)
	copy(buf[crcStart:], msgBuf)
	msgSet := commitlog.MessageSet(buf)
	return string(msgSet.Messages()[0].Key()),msgSet.Messages()[0].Value(), nil
}
