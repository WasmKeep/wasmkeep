//! HTTP headers as the wire carries them: an ordered list of (name, value)
//! pairs in which a name may repeat. Names compare without case.
//!
//! `get` is the first value, `get_all` every value in order, `append` adds,
//! `set` replaces every value. A `Set-Cookie` is never joined with a comma:
//! each stays its own pair. The pairs of `actors::Request` and
//! `actors::Response` are already lossless; wrap them with
//! `Headers::from(response.headers)` to look values up.

/// An ordered list of header pairs; see the [module](self).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Headers(Vec<(String, String)>);

impl Headers {
    /// No headers.
    pub fn new() -> Self {
        Self::default()
    }

    /// From raw values (as `wasi:http` fields give them): a value that is
    /// not UTF-8 is kept with its bad bytes replaced.
    pub fn from_byte_pairs(pairs: Vec<(String, Vec<u8>)>) -> Self {
        Self(
            pairs
                .into_iter()
                .map(|(k, v)| (k, String::from_utf8_lossy(&v).into_owned()))
                .collect(),
        )
    }

    /// The first value of `name`.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    /// Every value of `name`, in order.
    pub fn get_all<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a str> {
        self.0
            .iter()
            .filter(move |(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    /// Whether any value of `name` is present.
    pub fn contains(&self, name: &str) -> bool {
        self.get(name).is_some()
    }

    /// Adds a value after the others.
    pub fn append(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.0.push((name.into(), value.into()));
    }

    /// Replaces every value of `name` with `value`, at the place of its first.
    pub fn set(&mut self, name: impl Into<String>, value: impl Into<String>) {
        let name = name.into();
        let Some(at) = self
            .0
            .iter()
            .position(|(k, _)| k.eq_ignore_ascii_case(&name))
        else {
            self.0.push((name, value.into()));
            return;
        };
        let mut index = 0;
        self.0.retain(|(k, _)| {
            index += 1;
            index - 1 <= at || !k.eq_ignore_ascii_case(&name)
        });
        self.0[at] = (name, value.into());
    }

    /// Removes every value of `name`.
    pub fn remove(&mut self, name: &str) {
        self.0.retain(|(k, _)| !k.eq_ignore_ascii_case(name));
    }

    /// Every pair, in order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    /// The pairs, as `actors::Request` and `actors::Response` carry them.
    pub fn as_pairs(&self) -> &[(String, String)] {
        &self.0
    }

    /// The pairs, as `actors::Request` and `actors::Response` carry them.
    pub fn into_pairs(self) -> Vec<(String, String)> {
        self.0
    }

    /// How many pairs.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether there are none.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<Vec<(String, String)>> for Headers {
    fn from(pairs: Vec<(String, String)>) -> Self {
        Self(pairs)
    }
}

impl<'a> FromIterator<(&'a str, &'a str)> for Headers {
    fn from_iter<I: IntoIterator<Item = (&'a str, &'a str)>>(iter: I) -> Self {
        Self(
            iter.into_iter()
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXPIRES: &str = "a=1; Expires=Wed, 21 Oct 2026 07:28:00 GMT";

    fn cookies() -> Headers {
        Headers::from(vec![
            ("set-cookie".to_owned(), EXPIRES.to_owned()),
            ("x-other".to_owned(), "o".to_owned()),
            ("Set-Cookie".to_owned(), "b=2".to_owned()),
        ])
    }

    #[test]
    fn get_is_first_and_get_all_every_value_in_order_whatever_the_case() {
        let h = cookies();
        assert_eq!(h.get("SET-COOKIE"), Some(EXPIRES));
        assert_eq!(
            h.get_all("set-Cookie").collect::<Vec<_>>(),
            [EXPIRES, "b=2"]
        );
        assert_eq!(h.get("missing"), None);
        assert_eq!(h.get_all("missing").count(), 0);
        assert!(h.contains("X-OTHER"));
        assert_eq!(h.len(), 3);
    }

    #[test]
    fn append_adds_set_replaces_all_remove_removes_all() {
        let mut h = Headers::new();
        h.append("a", "1");
        h.append("A", "2");
        h.append("b", "x");
        assert_eq!(h.get_all("a").collect::<Vec<_>>(), ["1", "2"]);
        h.set("A", "3");
        assert_eq!(h.iter().collect::<Vec<_>>(), [("A", "3"), ("b", "x")]);
        h.set("c", "y");
        assert_eq!(h.get("c"), Some("y"));
        h.remove("B");
        assert!(!h.contains("b"));
    }

    #[test]
    fn pairs_round_trip_in_order_with_repeats() {
        let pairs = cookies().into_pairs();
        assert_eq!(pairs[0].0, "set-cookie");
        assert_eq!(pairs[2], ("Set-Cookie".to_owned(), "b=2".to_owned()));
        let from_bytes = Headers::from_byte_pairs(vec![
            ("set-cookie".to_owned(), b"a=1".to_vec()),
            ("set-cookie".to_owned(), b"b=2".to_vec()),
        ]);
        assert_eq!(
            from_bytes.get_all("set-cookie").collect::<Vec<_>>(),
            ["a=1", "b=2"]
        );
    }
}
