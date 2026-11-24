//! Files (ADR 0030): kept in the app's part of the bucket, or on the node's
//! disk without one, in the caller's own namespace, an actor's or the
//! front's. No call names another namespace.
//!
//! ```ignore
//! use wasmkeep_sdk::blobs::{self, PutOptions};
//!
//! blobs::put("photos/1.jpg", &bytes, &PutOptions::new().content_type("image/jpeg").if_absent())?;
//! if let Some((info, body)) = blobs::get("photos/1.jpg")? { /* up to 16 MiB */ }
//! for info in blobs::list_all("photos/") {
//!     let info = info?;
//! }
//! // Larger files: a stream each way.
//! let mut w = blobs::open_write("videos/1.mp4", &PutOptions::new())?;
//! std::io::Write::write_all(&mut w, &chunk)?;
//! let info = w.finish()?;
//! ```
//!
//! A key is 1 to 512 bytes of `[A-Za-z0-9._~-]` and `/`, in segments that
//! are not empty, `.` or `..`. A component imports `wasmkeep:actor/blobs`
//! only if it calls one of these functions. Fronts have them too
//! (`default-features = false`).

use std::io;

use crate::front::bindings::wasmkeep::actor::blobs as wit;
pub use wit::{BlobError, BlobInfo, Condition, Listing, UploadLink};

/// What a write stores and when: a content type (by default
/// `application/octet-stream`) and a condition (by default none).
#[derive(Debug, Clone)]
pub struct PutOptions {
    content_type: Option<String>,
    condition: Condition,
}

impl Default for PutOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl PutOptions {
    /// No content type, no condition.
    #[must_use]
    pub fn new() -> Self {
        Self {
            content_type: None,
            condition: Condition::Any,
        }
    }

    /// `type/subtype` with optional parameters.
    #[must_use]
    pub fn content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = Some(content_type.into());
        self
    }

    /// Write only if no file has this key (else [`BlobError::Conflict`]).
    #[must_use]
    pub fn if_absent(mut self) -> Self {
        self.condition = Condition::Absent;
        self
    }

    /// Write only if the file's ETag is `etag` (else
    /// [`BlobError::Conflict`]).
    #[must_use]
    pub fn if_match(mut self, etag: impl Into<String>) -> Self {
        self.condition = Condition::Etag(etag.into());
        self
    }

    fn wit(&self) -> wit::WriteOptions {
        wit::WriteOptions {
            content_type: self.content_type.clone(),
            condition: self.condition.clone(),
        }
    }
}

/// The file's information, or `None` if there is none.
///
/// # Errors
/// What the host refused or failed.
pub fn head(key: &str) -> Result<Option<BlobInfo>, BlobError> {
    wit::head(key)
}

/// The whole file, up to 16 MiB ([`BlobError::TooLarge`] past it: use
/// [`open_read`]), or `None`.
///
/// # Errors
/// What the host refused or failed.
pub fn get(key: &str) -> Result<Option<(BlobInfo, Vec<u8>)>, BlobError> {
    wit::get(key)
}

/// Writes the whole file, up to 16 MiB; returns once it is durable.
///
/// # Errors
/// [`BlobError::Conflict`] when the condition does not hold, and what else
/// the host refused or failed.
pub fn put(key: &str, body: &[u8], options: &PutOptions) -> Result<BlobInfo, BlobError> {
    wit::put(key, body, &options.wit())
}

/// Removes the file if it exists; durable when it returns.
///
/// # Errors
/// What the host refused or failed.
pub fn delete(key: &str) -> Result<(), BlobError> {
    wit::delete(key)
}

/// One page of the files whose key starts with `prefix` (as a string:
/// `up` finds `uploads/a`), in key order, after `after`, at most `limit`
/// (1 to 1,000). Pass the page's `next` as `after` for the next one.
///
/// # Errors
/// What the host refused or failed.
pub fn list(prefix: &str, after: Option<&str>, limit: u32) -> Result<Listing, BlobError> {
    wit::list(prefix, after, limit)
}

/// Every file whose key starts with `prefix`, page after page (1,000 at
/// a time). A page may be short, or empty, with more after it: this
/// follows `next`, never a page's length. An error ends the iteration
/// after it is yielded.
pub fn list_all(prefix: &str) -> ListAll {
    list_all_by(prefix, 1_000)
}

/// [`list_all`], `page` files a page.
pub fn list_all_by(prefix: &str, page: u32) -> ListAll {
    ListAll {
        prefix: prefix.to_owned(),
        page,
        after: None,
        entries: Vec::new().into_iter(),
        done: false,
    }
}

/// The iterator of [`list_all`].
#[derive(Debug)]
pub struct ListAll {
    prefix: String,
    page: u32,
    after: Option<String>,
    entries: std::vec::IntoIter<BlobInfo>,
    done: bool,
}

impl Iterator for ListAll {
    type Item = Result<BlobInfo, BlobError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(info) = self.entries.next() {
                return Some(Ok(info));
            }
            if self.done {
                return None;
            }
            match list(&self.prefix, self.after.as_deref(), self.page) {
                Ok(listing) => {
                    self.entries = listing.entries.into_iter();
                    match listing.next {
                        Some(next) => self.after = Some(next),
                        None => self.done = true,
                    }
                }
                Err(e) => {
                    self.done = true;
                    return Some(Err(e));
                }
            }
        }
    }
}

/// How a download link is served.
#[derive(Debug, Clone, Default)]
pub struct DownloadOptions {
    /// Seconds the link works: 300 by default, at most 3,600.
    pub expires_in_secs: Option<u32>,
    /// Shown by the browser instead of downloaded: only for images, audio,
    /// video and plain text ([`BlobError::NotAllowed`] otherwise).
    pub inline: bool,
    /// An attachment's file name (by default the key's last segment).
    pub filename: Option<String>,
}

/// A link a browser downloads the file from, without this code.
///
/// # Errors
/// What the host refused or failed.
pub fn link_download(key: &str, options: &DownloadOptions) -> Result<String, BlobError> {
    let disposition = if options.inline {
        wit::Disposition::Inline
    } else {
        wit::Disposition::Attachment(options.filename.clone())
    };
    wit::link_download(
        key,
        &wit::DownloadOptions {
            expires_in_secs: options.expires_in_secs,
            disposition,
        },
    )
}

/// A link a browser uploads one new file to, without this code: exactly
/// `size` bytes of `content_type`, within `expires_in_secs` (900 by
/// default, at most 3,600).
///
/// # Errors
/// What the host refused or failed.
pub fn link_upload(
    key: &str,
    size: u64,
    content_type: &str,
    expires_in_secs: Option<u32>,
) -> Result<UploadLink, BlobError> {
    wit::link_upload(
        key,
        &wit::UploadOptions {
            size,
            content_type: content_type.to_owned(),
            expires_in_secs,
        },
    )
}

/// A file being read: an [`io::Read`] of its bytes.
#[derive(Debug)]
pub struct Reader {
    stream: wasip2::io::streams::InputStream,
}

impl io::Read for Reader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        io::Read::read(&mut self.stream, buf)
    }
}

/// The file's information and a reader of its bytes `[start, start +
/// length)` (cut at the end), or of all of them; `None` if there is none.
/// The reader works within the event that opened it. At most 16 are open
/// at once (`not-allowed` past it): a reader counts until it is read to
/// its end or dropped, so one read only partly counts until dropped.
///
/// # Errors
/// What the host refused or failed.
pub fn open_read(
    key: &str,
    range: Option<(u64, u64)>,
) -> Result<Option<(BlobInfo, Reader)>, BlobError> {
    Ok(wit::open_read(key, range)?.map(|(info, stream)| (info, Reader { stream })))
}

/// A streamed write: an [`io::Write`] of the body, made the file by
/// [`Writer::finish`]. Dropped without it, the write is abandoned and
/// nothing is written.
#[derive(Debug)]
pub struct Writer {
    // Dropped before the upload is finished: the stream is the upload's.
    stream: Option<wasip2::io::streams::OutputStream>,
    upload: wit::Upload,
}

impl io::Write for Writer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // `check-write`, then `write` (never `blocking-write-and-flush`,
        // which takes at most 4,096 bytes).
        match self.stream.as_mut() {
            Some(s) => io::Write::write(s, buf),
            None => Err(io::ErrorKind::BrokenPipe.into()),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self.stream.as_mut() {
            Some(s) => io::Write::flush(s),
            None => Ok(()),
        }
    }
}

impl Writer {
    /// Ends the body and makes the file durable, with its condition.
    ///
    /// # Errors
    /// [`BlobError::TooLarge`] past the node's limit for one file,
    /// [`BlobError::Conflict`] when the condition does not hold, and what
    /// else the host refused or failed.
    pub fn finish(mut self) -> Result<BlobInfo, BlobError> {
        if let Some(mut s) = self.stream.take() {
            // What is buffered reaches the host; a failure shows in
            // `finish` (the spool's error).
            let _ = io::Write::flush(&mut s);
        }
        wit::Upload::finish(self.upload)
    }
}

/// Starts a streamed write of up to the node's limit for one file
/// (`--max-blob-bytes`, 1 GiB by default); nothing is visible until
/// [`Writer::finish`]. With a condition, at most 16 MiB.
///
/// # Errors
/// What the host refused or failed.
pub fn open_write(key: &str, options: &PutOptions) -> Result<Writer, BlobError> {
    let upload = wit::open_write(key, &options.wit())?;
    let stream = upload.body()?;
    Ok(Writer {
        stream: Some(stream),
        upload,
    })
}
