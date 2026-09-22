package kvstore

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
)

// WAL record frame layout (all integers little-endian):
//
//	crc32(4) | payloadLen(4) | payload
//
// payload:
//
//	seq(8) | opCount(4) | op...
//
// op:
//
//	type(1) | keyLen(4) | key | valLen(4) | val
//
// The CRC covers payloadLen and payload. One frame per committed
// transaction, so replay either applies a whole transaction or none of it.
const maxRecordSize = 1 << 30

const (
	opPut byte = 1
	opDel byte = 2
)

type op struct {
	typ byte
	key string
	val []byte
}

func encodeRecord(seq uint64, ops []op) []byte {
	n := 8 + 4
	for _, o := range ops {
		n += 1 + 4 + len(o.key) + 4 + len(o.val)
	}
	buf := make([]byte, 8+n)
	payload := buf[8:]
	binary.LittleEndian.PutUint64(payload[0:8], seq)
	binary.LittleEndian.PutUint32(payload[8:12], uint32(len(ops)))
	off := 12
	for _, o := range ops {
		payload[off] = o.typ
		off++
		binary.LittleEndian.PutUint32(payload[off:off+4], uint32(len(o.key)))
		off += 4
		copy(payload[off:], o.key)
		off += len(o.key)
		binary.LittleEndian.PutUint32(payload[off:off+4], uint32(len(o.val)))
		off += 4
		copy(payload[off:], o.val)
		off += len(o.val)
	}
	binary.LittleEndian.PutUint32(buf[4:8], uint32(n))
	binary.LittleEndian.PutUint32(buf[0:4], crc32.ChecksumIEEE(buf[4:]))
	return buf
}

func decodeRecord(payload []byte) (uint64, []op, error) {
	if len(payload) < 12 {
		return 0, nil, errors.New("kvstore: wal record too short")
	}
	seq := binary.LittleEndian.Uint64(payload[0:8])
	nops := binary.LittleEndian.Uint32(payload[8:12])
	ops := make([]op, 0, nops)
	off := 12
	for i := uint32(0); i < nops; i++ {
		if off+5 > len(payload) {
			return 0, nil, errors.New("kvstore: wal record truncated")
		}
		typ := payload[off]
		off++
		klen := int(binary.LittleEndian.Uint32(payload[off : off+4]))
		off += 4
		if off+klen+4 > len(payload) {
			return 0, nil, errors.New("kvstore: wal record truncated")
		}
		key := string(payload[off : off+klen])
		off += klen
		vlen := int(binary.LittleEndian.Uint32(payload[off : off+4]))
		off += 4
		if off+vlen > len(payload) {
			return 0, nil, errors.New("kvstore: wal record truncated")
		}
		val := append([]byte(nil), payload[off:off+vlen]...)
		off += vlen
		ops = append(ops, op{typ: typ, key: key, val: val})
	}
	return seq, ops, nil
}

// replayWAL reads frames from f (positioned at offset 0) and calls apply
// for every valid record whose seq is greater than afterSeq. Reading stops
// silently at the first corrupt or torn frame (a crash mid-write), and the
// returned offset is the end of the last valid frame so the caller can
// truncate the tail before appending.
func replayWAL(f *os.File, afterSeq uint64, apply func(seq uint64, ops []op) error) (int64, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	var off int64
	hdr := make([]byte, 8)
	for {
		_, err := io.ReadFull(f, hdr)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return off, nil
		}
		if err != nil {
			return off, err
		}
		crc := binary.LittleEndian.Uint32(hdr[0:4])
		n := binary.LittleEndian.Uint32(hdr[4:8])
		if n > maxRecordSize {
			return off, nil // corrupt length: treat as torn tail
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(f, payload); err != nil {
			return off, nil // torn tail
		}
		h := crc32.NewIEEE()
		h.Write(hdr[4:8])
		h.Write(payload)
		if h.Sum32() != crc {
			return off, nil // corrupt frame: stop replay
		}
		seq, ops, err := decodeRecord(payload)
		if err != nil {
			return off, nil
		}
		if seq > afterSeq {
			if err := apply(seq, ops); err != nil {
				return off, err
			}
		}
		off += 8 + int64(n)
	}
}

// wal is an append-only write-ahead log. All writes happen under DB.mu.
type wal struct {
	f    *os.File
	size int64
}

func (w *wal) append(rec []byte) error {
	n, err := w.f.Write(rec)
	w.size += int64(n)
	return err
}

func (w *wal) sync() error { return w.f.Sync() }

func (w *wal) close() error { return w.f.Close() }
