package kvstore

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// WAL record on-disk format (all integers little-endian):
//
//	[4B crc32 of payload][4B payload length][payload]
//
// payload:
//
//	[8B commitTS][4B numOps][op]...
//
// op:
//
//	[1B type: 0=put 1=delete][2B keyLen][key][4B valLen][val]
//
// One record per committed transaction. A record is either fully applied or
// fully ignored: a CRC/length mismatch terminates replay of that segment,
// which tolerates torn tail writes after a crash.

const walFilePattern = "wal-%06d.log"

type walWriter struct {
	f *os.File
}

func walPath(dir string, seq uint64) string {
	return filepath.Join(dir, fmt.Sprintf(walFilePattern, seq))
}

func openWalWriter(dir string, seq uint64) (*walWriter, error) {
	f, err := os.OpenFile(walPath(dir, seq), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &walWriter{f: f}, nil
}

func (w *walWriter) append(rec []byte, sync bool) error {
	if _, err := w.f.Write(rec); err != nil {
		return err
	}
	if sync {
		return w.f.Sync()
	}
	return nil
}

func (w *walWriter) close() error { return w.f.Close() }

// encodeRecord serializes one committed transaction into a WAL record.
func encodeRecord(ts uint64, writes map[string]writeOp) []byte {
	size := 8 + 4
	for k, op := range writes {
		size += 1 + 2 + len(k) + 4 + len(op.val)
	}
	buf := make([]byte, 8+size)
	payload := buf[8:]
	binary.LittleEndian.PutUint64(payload[0:8], ts)
	binary.LittleEndian.PutUint32(payload[8:12], uint32(len(writes)))
	off := 12
	for k, op := range writes {
		if op.del {
			payload[off] = 1
		}
		off++
		binary.LittleEndian.PutUint16(payload[off:off+2], uint16(len(k)))
		off += 2
		copy(payload[off:], k)
		off += len(k)
		binary.LittleEndian.PutUint32(payload[off:off+4], uint32(len(op.val)))
		off += 4
		copy(payload[off:], op.val)
		off += len(op.val)
	}
	binary.LittleEndian.PutUint32(buf[0:4], crc32.ChecksumIEEE(payload))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(size))
	return buf
}

// replayWalSegment parses a segment and applies every valid record with
// commitTS > cpTS via apply. It returns the largest commitTS seen. Parsing
// stops silently at the first corrupt or partial record (torn tail).
func replayWalSegment(path string, cpTS uint64, apply func(ts uint64, writes map[string]writeOp)) (uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	var maxTS uint64
	off := 0
	for off+8 <= len(data) {
		crc := binary.LittleEndian.Uint32(data[off : off+4])
		length := binary.LittleEndian.Uint32(data[off+4 : off+8])
		if length < 12 || off+8+int(length) > len(data) {
			break
		}
		payload := data[off+8 : off+8+int(length)]
		if crc32.ChecksumIEEE(payload) != crc {
			break
		}
		ts := binary.LittleEndian.Uint64(payload[0:8])
		numOps := binary.LittleEndian.Uint32(payload[8:12])
		writes := make(map[string]writeOp, numOps)
		p := 12
		ok := true
		for i := uint32(0); i < numOps; i++ {
			if p+3 > len(payload) {
				ok = false
				break
			}
			typ := payload[p]
			p++
			klen := int(binary.LittleEndian.Uint16(payload[p : p+2]))
			p += 2
			if p+klen+4 > len(payload) {
				ok = false
				break
			}
			key := string(payload[p : p+klen])
			p += klen
			vlen := int(binary.LittleEndian.Uint32(payload[p : p+4]))
			p += 4
			if p+vlen > len(payload) {
				ok = false
				break
			}
			val := append([]byte(nil), payload[p:p+vlen]...)
			p += vlen
			writes[key] = writeOp{val: val, del: typ == 1}
		}
		if !ok {
			break
		}
		if ts > cpTS {
			apply(ts, writes)
		}
		if ts > maxTS {
			maxTS = ts
		}
		off += 8 + int(length)
	}
	return maxTS, nil
}

// listWalSegments returns the sorted sequence numbers of all WAL segments in dir.
func listWalSegments(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var seqs []uint64
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "wal-") || !strings.HasSuffix(name, ".log") {
			continue
		}
		n, err := strconv.ParseUint(name[4:len(name)-4], 10, 64)
		if err != nil {
			continue
		}
		seqs = append(seqs, n)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	return seqs, nil
}
