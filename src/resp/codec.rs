use bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::{
    error::{DecodeError, RedisError},
    resp::RESPValue,
};

// The maximum buffer size when decoding to
// prevent overflowing server memory. About
// ~8 MB.
const MAX_BUFFER_SIZE: usize = 8 * 1024 * 1024;
const MAX_BULK_STRING_LENGTH: i64 = 2 * 1024 * 1024;
const MAX_ARRAY_LENGTH: i64 = 10_000;

macro_rules! try_incomplete {
    ($e:expr) => {
        match $e {
            Ok(Some(value)) => value,
            Ok(None) => return Ok(None),
            Err(e) => return Err(e)?,
        }
    };
}

macro_rules! try_optional {
    ($e:expr) => {
        match $e {
            Some(value) => value,
            None => return Ok(None),
        }
    };
}

/// A codec for the Redis serialization protocol (RESP2),
/// used for communication between clients and servers.
///
/// See the [specification](https://redis.io/docs/latest/develop/reference/protocol-spec/)
/// for more details.
pub struct RESPCodec;

impl RESPCodec {
    /// Checks if `src` contains enough data to parse a single
    /// RESP value starting at `pos`.
    ///
    /// Returns `Ok(Some(end))` where `src[pos..end]` is the
    /// complete serialized value, `Ok(None)` if more data is
    /// needed, or `Err` on malformed input.
    fn check(&self, src: &BytesMut, pos: usize) -> Result<Option<usize>, DecodeError> {
        if pos >= src.len() {
            return Ok(None);
        }

        // We can assume that the tag is present if we find a CRLF character
        // in the range [pos + 1, src.len())
        let crlf_offset = try_optional!(self.find_crlf(&src[pos + 1..]));
        let crlf_pos = pos + 1 + crlf_offset;
        let after_crlf_pos = crlf_pos + 2;
        match src[pos] {
            b'+' | b'-' | b':' => Ok(Some(after_crlf_pos)),
            b'$' => {
                let length = self.check_i64(&src[pos + 1..crlf_pos])?;
                if length == -1 {
                    // We found a valid null bulk string.
                    return Ok(Some(after_crlf_pos));
                } else if !(-1..=MAX_BULK_STRING_LENGTH).contains(&length) {
                    return Err(DecodeError::InvalidLength {
                        length,
                        min: -1,
                        max: MAX_BULK_STRING_LENGTH,
                    });
                }

                let end = after_crlf_pos + length as usize + 2;
                if src.len() < end {
                    Ok(None)
                } else {
                    Ok(Some(end))
                }
            }
            b'*' => {
                let length = self.check_i64(&src[pos + 1..crlf_pos])?;
                if length == -1 {
                    // We found a valid null array.
                    return Ok(Some(after_crlf_pos));
                } else if !(-1..=MAX_ARRAY_LENGTH).contains(&length) {
                    return Err(DecodeError::InvalidLength {
                        length,
                        min: -1,
                        max: MAX_ARRAY_LENGTH,
                    });
                }

                let mut cursor_pos = after_crlf_pos;
                for _ in 0..length {
                    cursor_pos = try_incomplete!(self.check(src, cursor_pos));
                }

                Ok(Some(cursor_pos))
            }
            tag => Err(DecodeError::UnknownTag { tag }),
        }
    }

    /// Parses a single RESP value from `src`, consuming the
    /// bytes that make up the value.
    ///
    /// Assumes that `src` contains a complete and correct value
    /// and, therefore, performs no checks. See [`check`](Self::check)
    /// to first validate this assumption.
    fn parse(&self, src: &mut BytesMut) -> RESPValue {
        let data_tag = src[0];
        src.advance(1);
        match data_tag {
            b'+' => RESPValue::SimpleString(self.parse_line(src)),
            b'-' => RESPValue::SimpleError(self.parse_line(src)),
            b':' => RESPValue::Integer(self.parse_i64(src)),
            b'$' => {
                let length = self.parse_i64(src);
                if length == -1 {
                    return RESPValue::NullBulkString;
                }

                let data = src.split_to(length as usize);
                src.advance(2);
                RESPValue::BulkString(data.freeze())
            }
            b'*' => {
                let length = self.parse_i64(src);
                if length == -1 {
                    return RESPValue::NullArray;
                }

                let mut values = Vec::with_capacity(length as usize);
                for _ in 0..length {
                    values.push(self.parse(src));
                }

                RESPValue::Array(values)
            }
            _ => unreachable!(),
        }
    }

    /// Finds the position of the first `\r\n` in the buffer
    /// or `None` if it was not found.
    fn find_crlf(&self, src: &[u8]) -> Option<usize> {
        memchr::memchr(b'\r', src).filter(|index| index + 1 < src.len() && src[index + 1] == b'\n')
    }

    /// Reads the line content between the start and the next `\r\n`
    /// exclusive and advances the internal cursor of `src` past the
    /// `\r\n`.
    fn parse_line(&self, src: &mut BytesMut) -> Bytes {
        let crlf = self
            .find_crlf(src)
            .expect("`check` should ensure sufficient bytes");
        let line = src.split_to(crlf);

        // Advance past the `\r\n` characters.
        src.advance(2);
        line.freeze()
    }

    /// Reads the line content between the start and the next `\r\n`
    /// exclusive and parses it as an `i64`.
    fn check_i64(&self, src: &[u8]) -> Result<i64, DecodeError> {
        let line = std::str::from_utf8(src).map_err(|_| DecodeError::BadInteger {
            bytes: src.to_vec(),
        })?;

        line.parse::<i64>().map_err(|_| DecodeError::BadInteger {
            bytes: src.to_vec(),
        })
    }

    /// Reads the line content between the start and the next `\r\n`
    /// exclusive and parses it as an `i64`, without any validation.
    fn parse_i64(&self, src: &mut BytesMut) -> i64 {
        let line = self.parse_line(src);
        std::str::from_utf8(&line)
            .expect("`check` should ensure valid integer")
            .parse::<i64>()
            .expect("`check` should ensure valid integer")
    }
}

impl Encoder<RESPValue> for RESPCodec {
    type Error = RedisError;

    fn encode(&mut self, item: RESPValue, dest: &mut BytesMut) -> Result<(), Self::Error> {
        item.encode(dest);
        Ok(())
    }
}

impl Decoder for RESPCodec {
    type Item = RESPValue;
    type Error = RedisError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        } else if src.len() >= MAX_BUFFER_SIZE {
            return Err(DecodeError::TooLarge {
                limit: MAX_BUFFER_SIZE,
            })?;
        }

        try_incomplete!(self.check(src, 0));
        Ok(Some(self.parse(src)))
    }
}
