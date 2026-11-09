//! Mapping addresses to file names.

use sha2::{Digest, Sha256};

/// Longest encoded segment kept verbatim; longer ones are shortened with a
/// hash so file names stay well under the usual 255-byte limit.
const MAX_ENCODED: usize = 200;

/// Encodes one address segment as a file name.
///
/// Bytes in `[a-z0-9_-]` and non-leading `.` are kept; every other byte
/// becomes `%XX` (uppercase letters included, so `Room` and `room` stay
/// distinct on case-insensitive file systems). The result never contains
/// `/`, never starts with `.`, and is at most about 235 bytes.
pub fn encode_segment(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    for (i, b) in segment.bytes().enumerate() {
        let keep = b.is_ascii_lowercase()
            || b.is_ascii_digit()
            || b == b'-'
            || b == b'_'
            || (b == b'.' && i > 0);
        if keep {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    if out.len() > MAX_ENCODED {
        let hash = Sha256::digest(segment.as_bytes());
        let hex: String = hash.iter().take(16).map(|b| format!("{b:02x}")).collect();
        // Cut on a char boundary (the encoded string is ASCII).
        out.truncate(MAX_ENCODED - 33);
        out.push('~');
        out.push_str(&hex);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::encode_segment;

    #[test]
    fn plain_names_stay_readable() {
        assert_eq!(encode_segment("room-1"), "room-1");
        assert_eq!(encode_segment("a.b_c"), "a.b_c");
    }

    #[test]
    fn unsafe_bytes_are_escaped() {
        assert_eq!(encode_segment("Room"), "%52oom");
        assert_eq!(encode_segment("a/b"), "a%2Fb");
        assert_eq!(encode_segment(".."), "%2E.");
        assert_eq!(encode_segment("é"), "%C3%A9");
        assert_ne!(encode_segment("Room"), encode_segment("room"));
    }

    #[test]
    fn long_names_are_hashed_and_distinct() {
        let a = "x".repeat(256);
        let b = format!("{}y", "x".repeat(255));
        let (ea, eb) = (encode_segment(&a), encode_segment(&b));
        assert!(
            ea.len() <= 200 && eb.len() <= 200,
            "{} {}",
            ea.len(),
            eb.len()
        );
        assert_ne!(ea, eb);
        let emoji = "😀".repeat(64); // 256 bytes, 12 encoded bytes each
        assert!(encode_segment(&emoji).len() <= 200);
    }
}
