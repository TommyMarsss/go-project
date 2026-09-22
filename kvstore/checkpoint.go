package kvstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
)

// Checkpoint file layout (all integers little-endian):
//
//	magic(8) "KVCKPT01" | seq(8) | count(8) | entry... | crc32(4)
//
// entry:
//
//	keyLen(4) | key | valLen(4) | val
//
// The CRC covers everything before it. The checkpoint stores, for every
// live key, its newest version as of seq; tombstones are omitted.
//
// A checkpoint is written to checkpoint.tmp, fsynced, then atomically
// renamed over checkpoint.dat, followed by a directory fsync. A crash at
// any point leaves either the old checkpoint.dat or an orphaned tmp file,
// never a half-written checkpoint.dat.
const (
	ckptMagic   = "KVCKPT01"
	ckptName    = "checkpoint.dat"
	ckptTmpName = "checkpoint.tmp"
)

var errCorruptCheckpoint = errors.New("kvstore: corrupt checkpoint file")

type ckptEntry struct {
	key string
	val []byte
}

func writeCheckpoint(dir string, seq uint64, entries []ckptEntry) error {
	var buf bytes.Buffer
	buf.WriteString(ckptMagic)
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], seq)
	buf.Write(tmp[:])
	binary.LittleEndian.PutUint64(tmp[:], uint64(len(entries)))
	buf.Write(tmp[:])
	for _, e := range entries {
		binary.LittleEndian.PutUint32(tmp[:4], uint32(len(e.key)))
		buf.Write(tmp[:4])
		buf.WriteString(e.key)
		binary.LittleEndian.PutUint32(tmp[:4], uint32(len(e.val)))
		buf.Write(tmp[:4])
		buf.Write(e.val)
	}
	binary.LittleEndian.PutUint32(tmp[:4], crc32.ChecksumIEEE(buf.Bytes()))
	buf.Write(tmp[:4])

	tmpPath := filepath.Join(dir, ckptTmpName)
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf.Bytes()); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, filepath.Join(dir, ckptName)); err != nil {
		return err
	}
	return syncDir(dir)
}

func readCheckpoint(path string) (uint64, []ckptEntry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, nil, err
	}
	if len(data) < 8+8+8+4 {
		return 0, nil, errCorruptCheckpoint
	}
	body, crcBytes := data[:len(data)-4], data[len(data)-4:]
	if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(crcBytes) {
		return 0, nil, errCorruptCheckpoint
	}
	if string(body[:8]) != ckptMagic {
		return 0, nil, errCorruptCheckpoint
	}
	seq := binary.LittleEndian.Uint64(body[8:16])
	count := binary.LittleEndian.Uint64(body[16:24])
	entries := make([]ckptEntry, 0, count)
	off := 24
	for i := uint64(0); i < count; i++ {
		if off+4 > len(body) {
			return 0, nil, errCorruptCheckpoint
		}
		klen := int(binary.LittleEndian.Uint32(body[off : off+4]))
		off += 4
		if off+klen+4 > len(body) {
			return 0, nil, errCorruptCheckpoint
		}
		key := string(body[off : off+klen])
		off += klen
		vlen := int(binary.LittleEndian.Uint32(body[off : off+4]))
		off += 4
		if off+vlen > len(body) {
			return 0, nil, errCorruptCheckpoint
		}
		val := append([]byte(nil), body[off:off+vlen]...)
		off += vlen
		entries = append(entries, ckptEntry{key: key, val: val})
	}
	return seq, entries, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
