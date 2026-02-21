use bytes::{Buf, BufMut, Bytes, BytesMut};
use itoa::Buffer;
use tokio_util::codec::{Decoder, Encoder};

use crate::{
    error::{DecodeError, RedisError},
    resp::{
        encoding::{self, CommandPartEncoding},
        parse::{
            check_i64, find_crlf, parse_i64, parse_line, try_incomplete, try_optional, BoolSlot,
            CommandArgumentStream, Slot,
        },
        ClientMessage, RESPValue, RedisCommand, SetCondition, SetExpiration,
    },
};

// The maximum buffer size when decoding to
// prevent overflowing server memory. About
// ~8 MB.
const MAX_BUFFER_SIZE: usize = 8 * 1024 * 1024;
const MAX_NESTING_DEPTH: usize = 100;
const MAX_BULK_STRING_LENGTH: i64 = 2 * 1024 * 1024;
const MAX_ARRAY_LENGTH: i64 = 10_000;

/// A codec for the Redis serialization protocol (RESP2),
/// used for communication between clients and servers.
///
/// See the [specification](https://redis.io/docs/latest/develop/reference/protocol-spec/)
/// for more details.
pub struct RESPCodec;

impl Encoder<RESPValue> for RESPCodec {
    type Error = RedisError;

    fn encode(&mut self, value: RESPValue, dest: &mut BytesMut) -> Result<(), Self::Error> {
        match value {
            RESPValue::SimpleString(bytes) => {
                dest.put_u8(b'+');
                dest.put(bytes);
                dest.extend_from_slice(b"\r\n");
            }
            RESPValue::SimpleError(bytes) => {
                dest.put_u8(b'-');
                dest.put(bytes);
                dest.extend_from_slice(b"\r\n");
            }
            RESPValue::Integer(value) => {
                let mut buf = Buffer::new();
                let value = buf.format(value);
                dest.put_u8(b':');
                dest.extend_from_slice(value.as_bytes());
                dest.extend_from_slice(b"\r\n");
            }
            RESPValue::NullBulkString => {
                dest.extend_from_slice(b"$-1\r\n");
            }
            RESPValue::BulkString(bytes) => {
                let mut buf = Buffer::new();
                let length = buf.format(bytes.len());
                dest.put_u8(b'$');
                dest.extend_from_slice(length.as_bytes());
                dest.extend_from_slice(b"\r\n");
                dest.put(bytes);
                dest.extend_from_slice(b"\r\n");
            }
            RESPValue::NullArray => {
                dest.extend_from_slice(b"*-1\r\n");
            }
            RESPValue::Array(values) => {
                let mut buf = Buffer::new();
                let length = buf.format(values.len());
                dest.put_u8(b'*');
                dest.extend_from_slice(length.as_bytes());
                dest.extend_from_slice(b"\r\n");
                for value in values {
                    self.encode(value, dest)?;
                }
            }
        }

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

        let end = try_incomplete!(self.check(src, 0, 0));
        let mut frame = src.split_to(end);
        Ok(Some(self.parse(&mut frame)))
    }
}

impl RESPCodec {
    /// Checks if `src` contains enough data to parse a single
    /// RESP value starting at `pos`.
    ///
    /// Returns `Ok(Some(end))` where `src[pos..end]` is the
    /// complete deserialized value, `Ok(None)` if more data is
    /// needed, or `Err` on malformed input.
    fn check(
        &self,
        src: &BytesMut,
        pos: usize,
        depth: usize,
    ) -> Result<Option<usize>, DecodeError> {
        if depth >= MAX_NESTING_DEPTH {
            return Err(DecodeError::TooDeep {
                limit: MAX_NESTING_DEPTH,
            });
        } else if pos >= src.len() {
            return Ok(None);
        }

        // We can assume that the tag is present if we find a CRLF character
        // in the range [pos + 1, src.len())
        let crlf_offset = try_optional!(find_crlf(&src[pos + 1..]));
        let crlf_pos = pos + 1 + crlf_offset;
        let after_crlf_pos = crlf_pos + 2;
        match src[pos] {
            b'+' | b'-' | b':' => Ok(Some(after_crlf_pos)),
            b'$' => {
                let length = check_i64(&src[pos + 1..crlf_pos])?;
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
                let length = check_i64(&src[pos + 1..crlf_pos])?;
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
                    cursor_pos = try_incomplete!(self.check(src, cursor_pos, depth + 1));
                }

                Ok(Some(cursor_pos))
            }
            tag => Err(DecodeError::UnknownTag { tag }),
        }
    }

    /// Parses a single RESP value from `src`, consuming the
    /// bytes that make up the value.
    ///
    /// Assumes [`check`](Self::check) has validated the structure
    /// and depth. MUST NOT be called without a successful `check()`.
    fn parse(&self, src: &mut BytesMut) -> RESPValue {
        let data_tag = src[0];
        src.advance(1);
        match data_tag {
            b'+' => RESPValue::SimpleString(parse_line(src)),
            b'-' => RESPValue::SimpleError(parse_line(src)),
            b':' => RESPValue::Integer(parse_i64(src)),
            b'$' => {
                let length = parse_i64(src);
                if length == -1 {
                    return RESPValue::NullBulkString;
                }

                let data = src.split_to(length as usize);
                src.advance(2);
                RESPValue::BulkString(data.freeze())
            }
            b'*' => {
                let length = parse_i64(src);
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
}

/// A codec for Redis commands, i.e. an array of
/// bulk strings.
///
/// See the [specification](https://redis.io/docs/latest/develop/reference/protocol-spec/#sending-commands-to-a-redis-server)
/// for more details.
pub struct RedisCommandCodec;

impl Encoder<RedisCommand> for RedisCommandCodec {
    type Error = RedisError;

    fn encode(&mut self, command: RedisCommand, dest: &mut BytesMut) -> Result<(), Self::Error> {
        let mut encoded_bytes = vec![];
        command.encode(&mut encoded_bytes);

        // Encode the length of the resulting array.
        let length = encoded_bytes.len();
        dest.put_u8(b'*');
        dest.extend_from_slice(Buffer::new().format(length).as_bytes());
        dest.extend_from_slice(b"\r\n");

        // The parts of the command have already been encoded
        // as a bulk string. So all that is left is to write them
        // in `dest`.
        let total_bytes = encoded_bytes.iter().map(Bytes::len).sum();
        dest.reserve(total_bytes);
        for bytes in encoded_bytes {
            dest.extend_from_slice(&bytes);
        }

        Ok(())
    }
}

impl Decoder for RedisCommandCodec {
    type Item = ClientMessage;
    type Error = RedisError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        } else if src.len() >= MAX_BUFFER_SIZE {
            return Err(DecodeError::TooLarge {
                limit: MAX_BUFFER_SIZE,
            })?;
        }

        let end = try_incomplete!(self.check(src, 0));

        // Split the complete frame from `src` before parsing. This
        // ensures the buffer stays clean even if `parse` fails
        // partway through (e.g. unknown command), so the stream
        // can continue decoding subsequent commands.
        let mut frame = src.split_to(end);
        Ok(Some(match self.parse(&mut frame) {
            Ok(command) => ClientMessage::Command(command),
            Err(err) => ClientMessage::Error(err),
        }))
    }
}

impl RedisCommandCodec {
    /// Checks if `src` contains enough data to parse a single
    /// Redis command starting at `pos`.
    ///
    /// Returns `Ok(Some(end))` where `src[pos..end]` is the
    /// complete deserialized command, `Ok(None)` if more data
    /// is needed, or `Err` on malformed input.
    fn check(&self, src: &BytesMut, pos: usize) -> Result<Option<usize>, DecodeError> {
        if pos >= src.len() {
            return Ok(None);
        }

        if src[pos] != b'*' {
            return Err(DecodeError::ExpectedArray { byte: src[pos] });
        }

        let crlf_offset = try_optional!(find_crlf(&src[pos + 1..]));
        let crlf_pos = pos + 1 + crlf_offset;
        let after_crlf_pos = crlf_pos + 2;
        let length = check_i64(&src[pos + 1..crlf_pos])?;
        if length < 1 || !(1..=MAX_ARRAY_LENGTH).contains(&length) {
            return Err(DecodeError::InvalidLength {
                length,
                min: 1,
                max: MAX_ARRAY_LENGTH,
            });
        }

        let mut cursor_pos = after_crlf_pos;
        for _ in 0..length {
            cursor_pos = try_incomplete!(self.check_part(src, cursor_pos));
        }

        Ok(Some(cursor_pos))
    }

    /// Checks if `src` contains enough data to parse a single
    /// bulk string for a Redis command starting at `pos`.
    ///
    /// Returns `Ok(Some(end))` where `src[pos..end]` is the
    /// complete deserialized bulk string, `Ok(None)` if more
    /// data is needed, or `Err` on malformed input.
    fn check_part(&self, src: &BytesMut, pos: usize) -> Result<Option<usize>, DecodeError> {
        if pos >= src.len() {
            return Ok(None);
        }

        if src[pos] != b'$' {
            return Err(DecodeError::ExpectedBulkString { byte: src[pos] });
        }

        let crlf_offset = try_optional!(find_crlf(&src[pos + 1..]));
        let crlf_pos = pos + 1 + crlf_offset;
        let after_crlf_pos = crlf_pos + 2;
        let length = check_i64(&src[pos + 1..crlf_pos])?;
        if length < 1 || !(1..=MAX_BULK_STRING_LENGTH).contains(&length) {
            return Err(DecodeError::InvalidLength {
                length,
                min: 1,
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

    /// Parses a single Redis command from `src`, consuming the
    /// bytes that make up the command.
    ///
    /// Assumes [`check`](Self::check) has validated that the
    /// command is an array of bulk strings. MUST NOT be called
    /// without a successful `check()`.
    fn parse(&self, src: &mut BytesMut) -> Result<RedisCommand, DecodeError> {
        debug_assert_eq!(src[0], b'*');
        src.advance(1);

        // We check that the length must be >= 1, so
        // we can safely convert this to a usize.
        let length = parse_i64(src) as usize;
        let mut args = CommandArgumentStream::new(src, length);
        let command = args.next()?;
        if command.eq_ignore_ascii_case(b"GET") {
            self.parse_get(args)
        } else if command.eq_ignore_ascii_case(b"SET") {
            self.parse_set(args)
        } else {
            Err(DecodeError::UnknownCommand { command })
        }
    }

    /// Parses a `GET` command.
    ///
    /// See [specification](https://redis.io/docs/latest/commands/get/)
    /// for more information.
    fn parse_get(&self, mut args: CommandArgumentStream<'_>) -> Result<RedisCommand, DecodeError> {
        let key = args.next()?;
        args.finish()?;

        Ok(RedisCommand::Get { key })
    }

    /// Parses a `SET` command.
    ///
    /// See [specification](https://redis.io/docs/latest/commands/set/)
    /// for more information.
    fn parse_set(&self, mut args: CommandArgumentStream<'_>) -> Result<RedisCommand, DecodeError> {
        let key = args.next()?;
        let value = args.next()?;

        let mut condition = Slot::<SetCondition>::new();
        let mut get = BoolSlot::new();
        let mut expiration = Slot::<SetExpiration>::new();
        while args.remaining() > 0 {
            let option = args.next()?;
            if option.eq_ignore_ascii_case(b"NX") {
                condition.set(SetCondition::Nx)?;
            } else if option.eq_ignore_ascii_case(b"XX") {
                condition.set(SetCondition::Xx)?;
            } else if option.eq_ignore_ascii_case(b"GET") {
                get.set()?;
            } else if option.eq_ignore_ascii_case(b"EX") {
                let seconds = args.next_i64()?;
                expiration.set(SetExpiration::Ex(seconds))?;
            } else if option.eq_ignore_ascii_case(b"PX") {
                let milliseconds = args.next_i64()?;
                expiration.set(SetExpiration::Px(milliseconds))?;
            } else if option.eq_ignore_ascii_case(b"EXAT") {
                let timestamp = args.next_i64()?;
                expiration.set(SetExpiration::ExAt(timestamp))?;
            } else if option.eq_ignore_ascii_case(b"PXAT") {
                let timestamp = args.next_i64()?;
                expiration.set(SetExpiration::PxAt(timestamp))?;
            } else if option.eq_ignore_ascii_case(b"KEEPTTL") {
                expiration.set(SetExpiration::KeepTtl)?;
            } else {
                return Err(DecodeError::UnknownCommandOption { option });
            }
        }

        args.finish()?;
        Ok(RedisCommand::Set {
            key,
            value,
            condition: condition.into_inner(),
            get: get.into_inner(),
            expiration: expiration.into_inner(),
        })
    }
}
