package call

import "testing"

// The messages of the Rust SDK's `wire::call`, case by case.
func TestFailureMessagesMatchRust(t *testing.T) {
	for _, c := range []struct{ kind, detail, want string }{
		{"invalid-address", "no class kv", "invalid address kv/shard-1: no class kv"},
		{"reentrant", "", "call to kv/shard-1 would re-enter an actor already in this call chain"},
		{"too-large", "", "call to kv/shard-1: body too large"},
		{"timeout", "", "call to kv/shard-1 timed out"},
		{"failed", "trapped", "call to kv/shard-1 failed: trapped"},
		{"something-new", "", "call to kv/shard-1 failed: something-new"},
		{"something-new", "why", "call to kv/shard-1 failed: something-new: why"},
	} {
		if got := Failure("kv/shard-1", c.kind, c.detail).Error(); got != c.want {
			t.Errorf("%s: got %q, want %q", c.kind, got, c.want)
		}
	}
}
