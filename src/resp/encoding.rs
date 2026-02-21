use bytes::{BufMut, Bytes, BytesMut};
use itoa::Buffer;
use tokio_util::codec::Encoder;

use crate::resp::{codec::RESPCodec, RESPValue};

pub fn simple_string(bytes: impl Into<Bytes>) -> RESPValue {
    RESPValue::SimpleString(bytes.into())
}

pub fn simple_error(bytes: impl Into<Bytes>) -> RESPValue {
    RESPValue::SimpleError(bytes.into())
}

pub fn integer(value: impl Into<i64>) -> RESPValue {
    RESPValue::Integer(value.into())
}

pub fn bulk_string(bytes: impl Into<Bytes>) -> RESPValue {
    RESPValue::BulkString(bytes.into())
}

pub fn array(values: Vec<RESPValue>) -> RESPValue {
    RESPValue::Array(values)
}

pub fn null() -> RESPValue {
    RESPValue::Null
}

/// Encodes a part of a Redis command in
/// bulk string format to a byte buffer.
///
/// Structually, a Redis command is an array
/// of bulk strings, with the first string
/// representing the name of the command.
///
/// Therefore, a "part" corresponds to one
/// or more bulk strings in the serialized
/// format and typically represents a single
/// option or flag.
pub trait CommandPartEncoding {
    /// Encodes this part to the `dest` as
    /// a bulk string.
    fn encode(self, dest: &mut Vec<Bytes>);
}

impl CommandPartEncoding for Bytes {
    fn encode(self, dest: &mut Vec<Bytes>) {
        let mut encoded = BytesMut::with_capacity(self.len());
        encoded.put_u8(b'$');
        encoded.extend_from_slice(Buffer::new().format(self.len()).as_bytes());
        encoded.extend_from_slice(b"\r\n");
        encoded.put(self);
        encoded.extend_from_slice(b"\r\n");
        dest.push(encoded.freeze());
    }
}

impl CommandPartEncoding for &'static str {
    fn encode(self, dest: &mut Vec<Bytes>) {
        Bytes::from_static(self.as_bytes()).encode(dest);
    }
}

impl CommandPartEncoding for i64 {
    fn encode(self, dest: &mut Vec<Bytes>) {
        let mut buf = Buffer::new();
        let value = buf.format(self);
        Bytes::copy_from_slice(value.as_bytes()).encode(dest);
    }
}

impl<T: CommandPartEncoding> CommandPartEncoding for Option<T> {
    fn encode(self, dest: &mut Vec<Bytes>) {
        if let Some(value) = self {
            value.encode(dest);
        }
    }
}

/// A TT muncher that implements [`CommandPartEncoding`] for
/// n-ary tuples and all smaller tuples. For example,
///
/// ```ignore
/// tuple_impls!(A = 0, B = 1, C = 2);
/// ```
///
/// will implement [`CommandPartEncoding`] for (A, B, C), (A, B)
/// and (A,).
macro_rules! tuple_impls {
    // Entry point
    (
        $( $T:ident = $idx:tt ),+ $(,)?
    ) => {
        tuple_impls!(@recurse [] ; $( $idx $T ),+);
    };

    // Recursive case: move one element from right into "done"
    (@recurse
        [ $( $done_idx:tt $done_T:ident ),* ]
        ;
        $idx:tt $T:ident $(, $rest_idx:tt $rest_T:ident )*
    ) => {
        // Recursively build smaller tuples for all "remaining"
        // elements.
        tuple_impls!(
            @recurse
            [ $( $done_idx $done_T, )* $idx $T ]
            ;
            $( $rest_idx $rest_T ),*
        );

        // Emit an implementation for the current "done" prefix
        tuple_impls!(@impl $( $done_idx $done_T, )* $idx $T);
    };

    // Base case: all items are "done".
    (@recurse
        [ $( $done_idx:tt $done_T:ident ),* ]
        ;
    ) => {};

    // "Private" internal implementation
    (@impl $( $idx:tt $T:ident ),+ ) => {
        impl<$( $T: CommandPartEncoding ),+> CommandPartEncoding for ( $( $T, )+ ) {
            #[inline]
            fn encode(self, dest: &mut Vec<Bytes>) {
                $( self.$idx.encode(dest); )+
            }
        }
    };
}

tuple_impls!(A = 0, B = 1, C = 2, D = 3);
