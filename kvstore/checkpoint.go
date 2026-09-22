package kvstore

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

// Checkpoint file format (all integers little-endian):
//
//	[8B magic "GPKVCP01"][8B checkpointTS][4B numEntries][entry]...[4B crc32 of all preceding bytes]
//
// entry:
//
//	[2B keyLen][key][4B valLen][val]
//
// A checkpoint is a full snapshot of the latest live (non-deleted) versions
// at checkpointTS. It is written to a temp file and atomically renamed, so a
// crash mid-checkpoint leaves the previous checkpoint untouched.
//
// The MANIFEST (JSON, also renamed atomically) records which checkpoint is
// valid and the oldest WAL segment still needed for recovery:
//
//	{"checkpoint_ts": N, "min_wal_seq": M}

const (
	checkpointMagic   = "GPKVCP01"
	checkpointFile    = "checkpoint.dat"
	checkpointTmpFile = "checkpoint.tmp"
	manifestFile      = "MANIFEST"
	manifestTmpFile   = "MANIFEST.tmp"
)

type manifest struct {
	CheckpointTS uint64 `json:"checkpoint_ts"`
	MinWalSeq    uint64 `json:"min_wal_seq"`
}

type checkpointEntry struct {
	key string
	val []byte
}

func writeCheckpointFile(dir string, ts uint64, ents []checkpointEntry) error {
	var buf bytes.Buffer
	buf.WriteString(checkpointMagic)
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], ts)
	buf.Write(tmp[:])
	binary.LittleEndian.PutUint32(tmp[:4], uint32(len(ents)))
	buf.Write(tmp[:4])
	for _, e := range ents {
		binary.LittleEndian.PutUint16(tmp[:2], uint16(len(e.key)))
		buf.Write(tmp[:2])
		buf.WriteString(e.key)
		binary.LittleEndian.PutUint32(tmp[:4], uint32(len(e.val)))
		buf.Write(tmp[:4])
		buf.Write(e.val)
	}
	binary.LittleEndian.PutUint32(tmp[:4], crc32.ChecksumIEEE(buf.Bytes()))
	buf.Write(tmp[:4])

	tmpPath := filepath.Join(dir, checkpointTmpFile)
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
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
	if err := os.Rename(tmpPath, filepath.Join(dir, checkpointFile)); err != nil {
		return err
	}
	return syncDir(dir)
}

func loadCheckpointFile(dir string) (uint64, []checkpointEntry, error) {
	data, err := os.ReadFile(filepath.Join(dir, checkpointFile))
	if os.IsNotExist(err) {
		return 0, nil, nil
	}
	if err != nil {
		return 0, nil, err
	}
	if len(data) < 8+8+4+4 || string(data[:8]) != checkpointMagic {
		return 0, nil, fmt.Errorf("kvstore: corrupt checkpoint header")
	}
	body, crcBytes := data[:len(data)-4], data[len(data)-4:]
	if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(crcBytes) {
		return 0, nil, fmt.Errorf("kvstore: checkpoint CRC mismatch")
	}
	ts := binary.LittleEndian.Uint64(body[8:16])
	n := binary.LittleEndian.Uint32(body[16:20])
	ents := make([]checkpointEntry, 0, n)
	p := 20
	for i := uint32(0); i < n; i++ {
		if p+2 > len(body) {
			return 0, nil, fmt.Errorf("kvstore: corrupt checkpoint entry")
		}
		klen := int(binary.LittleEndian.Uint16(body[p : p+2]))
		p += 2
		if p+klen+4 > len(body) {
			return 0, nil, fmt.Errorf("kvstore: corrupt checkpoint entry")
		}
		key := string(body[p : p+klen])
		p += klen
		vlen := int(binary.LittleEndian.Uint32(body[p : p+4]))
		p += 4
		if p+vlen > len(body) {
			return 0, nil, fmt.Errorf("kvstore: corrupt checkpoint entry")
		}
		ents = append(ents, checkpointEntry{key: key, val: append([]byte(nil), body[p:p+vlen]...)})
		p += vlen
	}
	return ts, ents, nil
}

func writeManifestFile(dir string, mf manifest) error {
	data, err := json.Marshal(mf)
	if err != nil {
		return err
	}
	tmpPath := filepath.Join(dir, manifestTmpFile)
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return err
	}
	if f, err := os.OpenFile(tmpPath, os.O_RDWR, 0o644); err == nil {
		f.Sync()
		f.Close()
	}
	if err := os.Rename(tmpPath, filepath.Join(dir, manifestFile)); err != nil {
		return err
	}
	return syncDir(dir)
}

func readManifestFile(dir string) (manifest, error) {
	var mf manifest
	data, err := os.ReadFile(filepath.Join(dir, manifestFile))
	if os.IsNotExist(err) {
		return mf, nil
	}
	if err != nil {
		return mf, err
	}
	if err := json.Unmarshal(data, &mf); err != nil {
		return mf, fmt.Errorf("kvstore: corrupt manifest: %w", err)
	}
	return mf, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
