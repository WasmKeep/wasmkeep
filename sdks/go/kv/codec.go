package kv

import (
	"sort"
	"strconv"

	"github.com/wasmkeep/wasmkeep/sdks/go/internal/wire"
)

// MaxKeyLen is the longest key, in bytes.
const MaxKeyLen = 1024

// Entry is a stored value.
type Entry struct {
	// Value is the bytes stored.
	Value []byte
	// Version changes on every write of the key, and is never reused
	// within a shard (not even after a delete), so a compare-and-set on an
	// old version always loses.
	Version uint64
	// ExpiresAt is when the key expires (ms since the Unix epoch), or 0 if
	// it does not.
	ExpiresAt uint64
}

func (e *Entry) clone() *Entry {
	if e == nil {
		return nil
	}
	c := *e
	c.Value = append([]byte{}, e.Value...)
	return &c
}

type expectKind uint8

const (
	expectAny expectKind = iota
	expectAbsent
	expectVersion
)

// Expect is what a conditional write expects to find: anything (the zero
// value, [Any]), no key or an expired one ([Absent]), or a version
// ([Version]).
type Expect struct {
	kind    expectKind
	version uint64
}

// Any expects nothing: the write is unconditional.
func Any() Expect { return Expect{} }

// Absent expects no key (or an expired one).
func Absent() Expect { return Expect{kind: expectAbsent} }

// Version expects the key at version v.
func Version(v uint64) Expect { return Expect{kind: expectVersion, version: v} }

// Allows reports whether a key at version current (present false: absent)
// meets e.
func (e Expect) Allows(current uint64, present bool) bool {
	switch e.kind {
	case expectAbsent:
		return !present
	case expectVersion:
		return present && current == e.version
	}
	return true
}

func (e Expect) encode() string {
	switch e.kind {
	case expectAbsent:
		return "absent"
	case expectVersion:
		return strconv.FormatUint(e.version, 10)
	}
	return "any"
}

// ShardFor is the shard (0-based) that holds key when there are shards of
// them: FNV-1a (64 bits) of the key's bytes, modulo the count (zero counts
// as one). Part of the data layout: it never changes, and matches the Rust
// and TypeScript SDKs.
func ShardFor(key string, shards uint32) uint32 {
	hash := uint64(0xcbf2_9ce4_8422_2325)
	for i := 0; i < len(key); i++ {
		hash ^= uint64(key[i])
		hash *= 0x0000_0100_0000_01b3
	}
	if shards == 0 {
		shards = 1
	}
	return uint32(hash % uint64(shards))
}

// ShardName is the actor name of shard index: `shard-<index>`.
func ShardName(index uint32) string {
	return "shard-" + strconv.FormatUint(uint64(index), 10)
}

func encodeEntry(e *Entry) []byte {
	var w wire.Writer
	return w.U64(e.Version).U64(e.ExpiresAt).Bytes(e.Value).Finish()
}

func decodeEntry(buf []byte) (*Entry, bool) {
	r := wire.NewReader(buf)
	version, ok1 := r.U64()
	expires, ok2 := r.U64()
	value, ok3 := r.Bytes()
	if !ok1 || !ok2 || !ok3 || !r.IsEmpty() {
		return nil, false
	}
	return &Entry{Value: value, Version: version, ExpiresAt: expires}, true
}

func encodeKeys(keys []string) []byte {
	var w wire.Writer
	w.U64(uint64(len(keys)))
	for _, k := range keys {
		w.Bytes([]byte(k))
	}
	return w.Finish()
}

func decodeKeys(buf []byte) ([]string, bool) {
	r := wire.NewReader(buf)
	n, ok := r.U64()
	if !ok {
		return nil, false
	}
	var keys []string
	for i := uint64(0); i < n; i++ {
		k, ok := r.Bytes()
		if !ok {
			return nil, false
		}
		keys = append(keys, string(k))
	}
	return keys, r.IsEmpty()
}

// mergePages merges sorted pages from every shard into one page of limit
// keys, in byte order (Go compares strings byte-wise, as Rust sorts
// Vec<u8>).
func mergePages(pages [][]string, limit uint32) []string {
	all := []string{}
	for _, p := range pages {
		all = append(all, p...)
	}
	sort.Strings(all)
	if uint64(len(all)) > uint64(limit) {
		all = all[:limit]
	}
	return all
}

type cached struct {
	at    uint64
	entry *Entry
}

// readCache is a bounded map of recent reads. When full, expired entries
// go first, then everything: simple, and a full cache is refilled by the
// next reads.
type readCache struct {
	capacity int
	entries  map[string]cached
}

func newReadCache(capacity int) *readCache {
	return &readCache{capacity: capacity, entries: map[string]cached{}}
}

// get returns the entry (nil: a cached absence) and true if it was read
// less than maxAge ago; an entry that expired since reads as absent. A
// miss returns false.
func (c *readCache) get(key string, now, maxAge uint64) (*Entry, bool) {
	hit, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	age := uint64(0)
	if now > hit.at {
		age = now - hit.at
	}
	if age >= maxAge {
		return nil, false
	}
	e := hit.entry
	if e == nil || (e.ExpiresAt != 0 && e.ExpiresAt <= now) {
		return nil, true
	}
	return e.clone(), true
}

// insert keeps a copy of entry (nil: absent), read at now.
func (c *readCache) insert(key string, now uint64, entry *Entry) {
	if _, ok := c.entries[key]; !ok && len(c.entries) >= c.capacity {
		for k, v := range c.entries {
			if v.entry != nil && v.entry.ExpiresAt != 0 && v.entry.ExpiresAt <= now {
				delete(c.entries, k)
			}
		}
		if len(c.entries) >= c.capacity {
			c.entries = map[string]cached{}
		}
	}
	c.entries[key] = cached{at: now, entry: entry.clone()}
}

func (c *readCache) remove(key string) {
	delete(c.entries, key)
}
