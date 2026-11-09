// Command ltx-conformance checks WasmKeep's LTX codec against the reference
// implementation (github.com/superfly/ltx), in both directions:
//
//	go run . gen DIR       write fixture files with the reference encoder
//	go run . verify FILE…  verify files (written by WasmKeep) with the reference decoder
//
// Page contents are a pure function of (pgno, i) so the Rust tests can
// recompute them: byte(i*31 + pgno*7 + seed).
package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/superfly/ltx"
)

func page(pgno uint32, size uint32, seed byte) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(uint32(i)*31+pgno*7) + seed
	}
	return b
}

type fixture struct {
	name     string
	header   ltx.Header
	pgnos    []uint32
	seed     byte
	preSeed  byte // pages before this file, for the post-apply checksum
	snapshot bool
}

func checksumOf(pageSize, commit uint32, pages map[uint32][]byte) ltx.Checksum {
	c := ltx.ChecksumFlag
	lock := ltx.LockPgno(pageSize)
	for pgno := uint32(1); pgno <= commit; pgno++ {
		if pgno == lock {
			continue
		}
		c = ltx.ChecksumFlag | (c ^ ltx.ChecksumPage(pgno, pages[pgno]))
	}
	return c
}

func gen(dir string) error {
	const ts = 1700000000000
	type spec struct {
		name     string
		pageSize uint32
		commit   uint32
		minTXID  ltx.TXID
		maxTXID  ltx.TXID
		pgnos    []uint32 // nil: snapshot of all pages
		flags    uint32
		nodeID   uint64
	}
	specs := []spec{
		{"snapshot-512", 512, 4, 1, 1, nil, 0, 0},
		{"snapshot-4096", 4096, 3, 1, 5, nil, 0, 7},
		{"incremental-4096", 4096, 6, 6, 9, []uint32{2, 5, 6}, 0, 7},
		{"no-checksum", 1024, 2, 3, 3, []uint32{1}, ltx.HeaderFlagNoChecksum, 0},
	}
	for _, s := range specs {
		// The database before: pages 1..commit with seed 0. The file
		// rewrites its pages with seed 1 (a snapshot rewrites all of them).
		before := map[uint32][]byte{}
		after := map[uint32][]byte{}
		for p := uint32(1); p <= s.commit; p++ {
			before[p] = page(p, s.pageSize, 0)
			after[p] = before[p]
		}
		pgnos := s.pgnos
		if pgnos == nil {
			for p := uint32(1); p <= s.commit; p++ {
				pgnos = append(pgnos, p)
			}
		}
		for _, p := range pgnos {
			after[p] = page(p, s.pageSize, 1)
		}
		hdr := ltx.Header{
			Version: ltx.Version, Flags: s.flags, PageSize: s.pageSize, Commit: s.commit,
			MinTXID: s.minTXID, MaxTXID: s.maxTXID, Timestamp: ts, NodeID: s.nodeID,
		}
		var post ltx.Checksum
		if s.flags&ltx.HeaderFlagNoChecksum == 0 {
			if s.minTXID != 1 {
				hdr.PreApplyChecksum = checksumOf(s.pageSize, s.commit, before)
			}
			post = checksumOf(s.pageSize, s.commit, after)
		}
		spec := ltx.FileSpec{Header: hdr, Trailer: ltx.Trailer{PostApplyChecksum: post}}
		for _, p := range pgnos {
			spec.Pages = append(spec.Pages, ltx.PageSpec{Header: ltx.PageHeader{Pgno: p}, Data: after[p]})
		}
		var buf bytes.Buffer
		if _, err := spec.WriteTo(&buf); err != nil {
			return fmt.Errorf("%s: %w", s.name, err)
		}
		if err := os.WriteFile(filepath.Join(dir, s.name+".ltx"), buf.Bytes(), 0o644); err != nil {
			return err
		}
		fmt.Printf("%s: pre=%s post=%s file=%s\n", s.name, hdr.PreApplyChecksum, post, spec.Trailer.FileChecksum)
	}
	return nil
}

func verify(paths []string) error {
	if len(paths) == 0 {
		return fmt.Errorf("no files to verify")
	}
	for _, p := range paths {
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		dec := ltx.NewDecoder(f)
		err = dec.Verify()
		_ = f.Close()
		if err != nil {
			return fmt.Errorf("%s: %w", p, err)
		}
		h, t := dec.Header(), dec.Trailer()
		fmt.Printf("%s: ok (txid %s-%s, commit %d, post %s)\n", p, h.MinTXID, h.MaxTXID, h.Commit, t.PostApplyChecksum)
	}
	return nil
}

func main() {
	var err error
	switch {
	case len(os.Args) == 3 && os.Args[1] == "gen":
		err = gen(os.Args[2])
	case len(os.Args) >= 2 && os.Args[1] == "verify":
		err = verify(os.Args[2:])
	default:
		err = fmt.Errorf("usage: ltx-conformance gen DIR | verify FILE...")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
