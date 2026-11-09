// Package wire is the small binary framing the kv and queue clients speak
// with the actors that hold the data (the Rust SDK's wire.rs, byte for
// byte): unsigned integers as 8 bytes big-endian, byte strings as a 4-byte
// big-endian length then the bytes. Internal to the SDK; the shapes follow
// the protocol in ADR 0015.
package wire

import (
	"encoding/binary"
	"strings"
	"unicode/utf8"
)

// Writer appends to a buffer.
type Writer struct {
	buf []byte
}

// U64 appends v as 8 bytes, big-endian.
func (w *Writer) U64(v uint64) *Writer {
	w.buf = binary.BigEndian.AppendUint64(w.buf, v)
	return w
}

// Bytes appends v's length (4 bytes), then v.
func (w *Writer) Bytes(v []byte) *Writer {
	// Bodies are capped at 16 MB by `actors.call`, so a length always fits.
	n := uint64(len(v))
	if n > 0xffff_ffff {
		n = 0xffff_ffff
	}
	w.buf = binary.BigEndian.AppendUint32(w.buf, uint32(n))
	w.buf = append(w.buf, v...)
	return w
}

// Finish returns what was written.
func (w *Writer) Finish() []byte {
	if w.buf == nil {
		return []byte{}
	}
	return w.buf
}

// Reader reads what Writer wrote. Every read fails (ok false) past the end.
type Reader struct {
	buf []byte
}

// NewReader reads buf.
func NewReader(buf []byte) *Reader {
	return &Reader{buf: buf}
}

// U64 reads 8 bytes, big-endian.
func (r *Reader) U64() (uint64, bool) {
	if len(r.buf) < 8 {
		return 0, false
	}
	v := binary.BigEndian.Uint64(r.buf)
	r.buf = r.buf[8:]
	return v, true
}

// Bytes reads a length-prefixed byte string, copied out of the buffer. A
// length past the end fails rather than truncating.
func (r *Reader) Bytes() ([]byte, bool) {
	if len(r.buf) < 4 {
		return nil, false
	}
	n := uint64(binary.BigEndian.Uint32(r.buf))
	if uint64(len(r.buf)-4) < n {
		return nil, false
	}
	v := make([]byte, n)
	copy(v, r.buf[4:4+n])
	r.buf = r.buf[4+n:]
	return v, true
}

// IsEmpty reports whether everything was read.
func (r *Reader) IsEmpty() bool {
	return len(r.buf) == 0
}

const digits = "0123456789abcdef"

// Hex is lowercase hex, two digits per byte, so any key fits in a path
// segment.
func Hex(b []byte) string {
	out := make([]byte, 0, 2*len(b))
	for _, c := range b {
		out = append(out, digits[c>>4], digits[c&0x0f])
	}
	return string(out)
}

// Lossy is a refusal's text: the body as UTF-8, invalid bytes replaced
// with U+FFFD, trimmed.
func Lossy(body []byte) string {
	if utf8.Valid(body) {
		return strings.TrimSpace(string(body))
	}
	var b strings.Builder
	for len(body) > 0 {
		r, n := utf8.DecodeRune(body)
		if r == utf8.RuneError && n <= 1 {
			b.WriteRune(utf8.RuneError)
		} else {
			b.Write(body[:n])
		}
		body = body[n:]
	}
	return strings.TrimSpace(b.String())
}
