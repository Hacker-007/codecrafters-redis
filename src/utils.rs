use bytes::Bytes;
use xxhash_rust::xxh3;

/// Returns the hex-encoded 64-bit XX3 hash of `bytes`.
///
/// See [`xxHash`](https://github.com/Cyan4973/xxHash) for
/// more information.
pub fn digest(bytes: &[u8]) -> Bytes {
    let hashed = xxh3::xxh3_64(bytes);
    Bytes::from(format!("{hashed:x}"))
}
