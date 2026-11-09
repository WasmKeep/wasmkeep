//! The key-value shard class of the jobs example: the SDK's ready-made
//! shard, deployed as class `kv`. Each actor `kv/shard-<n>` holds the keys
//! that hash to it, in its own SQLite database; only other actors may call
//! it (an outside client gets `403`).

use wasmkeep_sdk::kv::Shard;

wasmkeep_sdk::export!(Shard);
