// Package kv is key-value storage built from actors (ADR 0015): the client
// side.
//
// Keys are spread over a fixed number of shard actors of one class
// (`<class>/shard-0` .. `shard-<n-1>`), chosen by FNV-1a 64 of the key
// modulo n. Each shard keeps its keys in its own SQLite database, so every
// operation on a key is strongly consistent. The shard class is the Rust
// SDK's ready-made shard, deployed from examples/jobs/kv; this client
// speaks its protocol (`/wasmkeep-kv/v1`) through the app's [call.Caller].
//
//	store := kv.New("kv", 8, callActor)
//	version, err := store.Put("user:42", []byte("Ada"))
//	entry, err := store.Get("user:42")      // nil if absent
//	_, err = store.CompareAndSet("user:42", kv.Version(version), []byte("Ada L."))
//	if errors.Is(err, kv.ErrConflict) { /* it moved */ }
//
// Keys are Go strings, which hold any bytes: a string key and the same
// bytes from another SDK reach the same shard.
package kv

import (
	"strconv"
	"time"

	"github.com/wasmkeep/wasmkeep/sdks/go/call"
	"github.com/wasmkeep/wasmkeep/sdks/go/internal/wire"
)

const (
	prefix     = "/wasmkeep-kv/v1"
	hdrExpect  = "x-wasmkeep-kv-expect"
	hdrTTL     = "x-wasmkeep-kv-ttl-ms"
	hdrDurable = "x-wasmkeep-kv-durable"
)

// Kind is why a KV operation failed.
type Kind uint8

const (
	// Conflict: a conditional write found another version (or a key where
	// it expected none).
	Conflict Kind = iota + 1
	// KeyTooLong: the key is longer than [MaxKeyLen] bytes.
	KeyTooLong
	// Storage: the shard's database refused the operation.
	Storage
	// Call: the call to the shard failed. The outcome of a write is then
	// not known (it may have applied): re-read before retrying a
	// conditional write.
	Call
	// Refused: the shard answered with an unexpected status.
	Refused
)

// Error is a failed KV operation. Match a kind with errors.Is and the
// sentinels ([ErrConflict], ...), or read the fields with errors.As.
type Error struct {
	Kind Kind
	// Status is the shard's status, for Refused.
	Status uint16
	// Detail is the call's error (Call), the database's message
	// (Storage), or the shard's answer (Refused).
	Detail string
}

// The sentinels, one per kind: errors.Is(err, kv.ErrConflict) holds for
// any *Error of that kind.
var (
	ErrConflict   = &Error{Kind: Conflict}
	ErrKeyTooLong = &Error{Kind: KeyTooLong}
	ErrStorage    = &Error{Kind: Storage}
	ErrCall       = &Error{Kind: Call}
	ErrRefused    = &Error{Kind: Refused}
)

// Error is the Rust SDK's message for the same error.
func (e *Error) Error() string {
	switch e.Kind {
	case Conflict:
		return "conflict: the key changed"
	case KeyTooLong:
		return "key longer than " + strconv.Itoa(MaxKeyLen) + " bytes"
	case Storage:
		return "storage: " + e.Detail
	case Call:
		return e.Detail
	}
	return "shard answered " + strconv.FormatUint(uint64(e.Status), 10) + ": " + e.Detail
}

// Is matches any error of the same kind.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Kind == e.Kind
}

func badReply() error {
	return &Error{Kind: Refused, Status: 200, Detail: "malformed reply from shard"}
}

func refused(status uint16, body []byte) error {
	return &Error{Kind: Refused, Status: status, Detail: wire.Lossy(body)}
}

func callFailed(err error) error {
	return &Error{Kind: Call, Detail: err.Error()}
}

func checkKey(key string) error {
	if len(key) > MaxKeyLen {
		return ErrKeyTooLong
	}
	return nil
}

// One per instance: lost with it (hibernation, a failed event, a deploy).
var cache = newReadCache(4096)

// nowMs is the wall clock in ms since the Unix epoch (a variable, so the
// tests can move it).
var nowMs = func() uint64 { return uint64(time.Now().UnixMilli()) }

// PutOptions are the options of [Client.PutWith]. The zero value is a
// plain durable write.
type PutOptions struct {
	// Expect is the condition the write needs (zero value: none).
	Expect Expect
	// TTLMs expires the key this many milliseconds after the write; 0
	// means never.
	TTLMs uint64
	// Local acknowledges the write once committed on the shard's node,
	// without waiting for it to be durable (in the bucket in a cluster,
	// fsynced on one node): faster, but a node lost in that window loses
	// it. The default (false) waits.
	Local bool
}

// Client reaches the shards of one class. Cheap to create; keep one per
// class. Not safe for concurrent use (an actor runs one event at a time).
type Client struct {
	class   string
	shards  uint32
	call    call.Caller
	cacheMs uint64
}

// New spreads keys over shards actors of class (at least one), reached
// through c. Every caller must use the same count.
func New(class string, shards uint32, c call.Caller) *Client {
	if shards == 0 {
		shards = 1
	}
	return &Client{class: class, shards: shards, call: c}
}

// WithReadCache serves [Client.Get] from an in-memory cache in this
// actor's instance when the entry was read less than maxAgeMs ago. This
// actor's own writes through the client update the cache, so it reads its
// own writes; other actors' writes show up within maxAgeMs. The cache is
// lost with the instance. Returns the client.
func (c *Client) WithReadCache(maxAgeMs uint64) *Client {
	c.cacheMs = maxAgeMs
	return c
}

// Class is the shard class.
func (c *Client) Class() string { return c.class }

// Shards is the shard count.
func (c *Client) Shards() uint32 { return c.shards }

func (c *Client) address(key string) string {
	return c.class + "/" + ShardName(ShardFor(key, c.shards))
}

func (c *Client) cacheKey(key string) string {
	return c.class + "\x00" + key
}

func (c *Client) remember(key string, entry *Entry) {
	if c.cacheMs > 0 {
		cache.insert(c.cacheKey(key), nowMs(), entry)
	}
}

func (c *Client) forget(key string) {
	if c.cacheMs > 0 {
		cache.remove(c.cacheKey(key))
	}
}

// Get returns the entry at key (nil if absent or expired), from the read
// cache if one is set and fresh.
func (c *Client) Get(key string) (*Entry, error) {
	if c.cacheMs > 0 {
		if e, ok := cache.get(c.cacheKey(key), nowMs(), c.cacheMs); ok {
			return e, nil
		}
	}
	return c.GetFresh(key)
}

// GetFresh returns the entry at key (nil if absent or expired), always
// asking the shard.
func (c *Client) GetFresh(key string) (*Entry, error) {
	if err := checkKey(key); err != nil {
		return nil, err
	}
	status, body, err := c.call(c.address(key), "GET", prefix+"/k/"+wire.Hex([]byte(key)), [][2]string{}, []byte{})
	if err != nil {
		return nil, callFailed(err)
	}
	var entry *Entry
	switch status {
	case 200:
		e, ok := decodeEntry(body)
		if !ok {
			return nil, badReply()
		}
		entry = e
	case 404:
	default:
		return nil, refused(status, body)
	}
	c.remember(key, entry)
	return entry, nil
}

// Put stores value at key (durably); returns the new version.
func (c *Client) Put(key string, value []byte) (uint64, error) {
	return c.PutWith(key, value, PutOptions{})
}

// CompareAndSet stores value only if key meets expected ([Absent] or
// [Version]); an error of kind [Conflict] otherwise.
func (c *Client) CompareAndSet(key string, expected Expect, value []byte) (uint64, error) {
	return c.PutWith(key, value, PutOptions{Expect: expected})
}

// PutWith stores value at key with options; returns the new version.
func (c *Client) PutWith(key string, value []byte, options PutOptions) (uint64, error) {
	if err := checkKey(key); err != nil {
		return 0, err
	}
	durable := "1"
	if options.Local {
		durable = "0"
	}
	headers := [][2]string{{hdrExpect, options.Expect.encode()}, {hdrDurable, durable}}
	if options.TTLMs > 0 {
		headers = append(headers, [2]string{hdrTTL, strconv.FormatUint(options.TTLMs, 10)})
	}
	if value == nil {
		value = []byte{}
	}
	sent := nowMs()
	status, body, err := c.call(c.address(key), "PUT", prefix+"/k/"+wire.Hex([]byte(key)), headers, value)
	if err != nil {
		c.forget(key)
		return 0, callFailed(err)
	}
	switch status {
	case 200:
		version, ok := wire.NewReader(body).U64()
		if !ok {
			return 0, badReply()
		}
		entry := &Entry{Value: value, Version: version}
		if options.TTLMs > 0 {
			entry.ExpiresAt = saturatingAdd(sent, options.TTLMs)
		}
		c.remember(key, entry)
		return version, nil
	case 409:
		c.forget(key)
		return 0, ErrConflict
	}
	c.forget(key)
	return 0, refused(status, body)
}

func saturatingAdd(a, b uint64) uint64 {
	if a+b < a {
		return ^uint64(0)
	}
	return a + b
}

// Delete removes key; true if it was there.
func (c *Client) Delete(key string) (bool, error) {
	return c.deleteWith(key, Any())
}

// DeleteIf removes key only if it holds version; an error of kind
// [Conflict] otherwise.
func (c *Client) DeleteIf(key string, version uint64) (bool, error) {
	return c.deleteWith(key, Version(version))
}

func (c *Client) deleteWith(key string, expect Expect) (bool, error) {
	if err := checkKey(key); err != nil {
		return false, err
	}
	headers := [][2]string{{hdrExpect, expect.encode()}}
	status, body, err := c.call(c.address(key), "DELETE", prefix+"/k/"+wire.Hex([]byte(key)), headers, []byte{})
	if err != nil {
		c.forget(key)
		return false, callFailed(err)
	}
	switch status {
	case 200:
		removed, ok := wire.NewReader(body).U64()
		if !ok {
			return false, badReply()
		}
		c.remember(key, nil)
		return removed == 1, nil
	case 409:
		c.forget(key)
		return false, ErrConflict
	}
	c.forget(key)
	return false, refused(status, body)
}

// List returns up to limit keys starting with prefix, in byte order. It
// asks every shard (one call each), so it costs more than a read; keep it
// for listings, not hot paths.
func (c *Client) List(prefix string, limit uint32) ([]string, error) {
	return c.list(prefix, nil, limit)
}

// ListAfter is [Client.List] from after (exclusive): pass the last key of
// a page to get the next one.
func (c *Client) ListAfter(prefix, after string, limit uint32) ([]string, error) {
	return c.list(prefix, &after, limit)
}

func (c *Client) list(keyPrefix string, after *string, limit uint32) ([]string, error) {
	path := prefix + "/list?prefix=" + wire.Hex([]byte(keyPrefix)) + "&limit=" + strconv.FormatUint(uint64(limit), 10)
	if after != nil {
		path += "&after=" + wire.Hex([]byte(*after))
	}
	pages := make([][]string, 0, c.shards)
	for index := uint32(0); index < c.shards; index++ {
		status, body, err := c.call(c.class+"/"+ShardName(index), "GET", path, [][2]string{}, []byte{})
		if err != nil {
			return nil, callFailed(err)
		}
		if status != 200 {
			return nil, refused(status, body)
		}
		keys, ok := decodeKeys(body)
		if !ok {
			return nil, badReply()
		}
		pages = append(pages, keys)
	}
	return mergePages(pages, limit), nil
}
