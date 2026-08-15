//! Bencode parser for .torrent files

use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq)]
pub enum Bencode {
    Integer(i64),
    Bytes(Vec<u8>),
    List(Vec<Bencode>),
    Dict(BTreeMap<String, Bencode>),
}

#[derive(Debug, thiserror::Error)]
pub enum BencodeError {
    #[error("unexpected end of input")]
    UnexpectedEof,
    #[error("invalid integer: {0}")]
    InvalidInteger(String),
    #[error("invalid string length: {0}")]
    InvalidLength(String),
    #[error("invalid byte sequence")]
    InvalidBytes,
    #[error("unexpected character: {0}")]
    UnexpectedChar(char),
    #[error("invalid format")]
    InvalidFormat,
}

pub fn decode(input: &[u8]) -> Result<Bencode, BencodeError> {
    let (value, _) = decode_inner(input, 0)?;
    Ok(value)
}

/// Encode a Bencode value back to bytes
pub fn encode(value: &Bencode) -> Vec<u8> {
    match value {
        Bencode::Integer(n) => format!("i{}e", n).into_bytes(),
        Bencode::Bytes(b) => {
            let mut result = format!("{}:", b.len()).into_bytes();
            result.extend_from_slice(b);
            result
        }
        Bencode::List(items) => {
            let mut result = vec![b'l'];
            for item in items {
                result.extend(encode(item));
            }
            result.push(b'e');
            result
        }
        Bencode::Dict(map) => {
            let mut result = vec![b'd'];
            for (key, value) in map {
                result.extend(encode(&Bencode::Bytes(key.as_bytes().to_vec())));
                result.extend(encode(value));
            }
            result.push(b'e');
            result
        }
    }
}

fn decode_inner(input: &[u8], pos: usize) -> Result<(Bencode, usize), BencodeError> {
    if pos >= input.len() {
        return Err(BencodeError::UnexpectedEof);
    }

    match input[pos] {
        b'i' => decode_integer(input, pos),
        b'l' => decode_list(input, pos),
        b'd' => decode_dict(input, pos),
        b'0'..=b'9' => decode_bytes(input, pos),
        c => Err(BencodeError::UnexpectedChar(c as char)),
    }
}

fn decode_integer(input: &[u8], pos: usize) -> Result<(Bencode, usize), BencodeError> {
    let start = pos + 1;
    let mut end = start;

    while end < input.len() && input[end] != b'e' {
        end += 1;
    }

    if end >= input.len() {
        return Err(BencodeError::UnexpectedEof);
    }

    let num_str = std::str::from_utf8(&input[start..end])
        .map_err(|_| BencodeError::InvalidInteger(format!("{:?}", &input[start..end])))?;
    let num: i64 = num_str
        .parse()
        .map_err(|_| BencodeError::InvalidInteger(num_str.to_string()))?;

    Ok((Bencode::Integer(num), end + 1))
}

fn decode_bytes(input: &[u8], pos: usize) -> Result<(Bencode, usize), BencodeError> {
    let mut colon_pos = pos;
    while colon_pos < input.len() && input[colon_pos] != b':' {
        colon_pos += 1;
    }

    if colon_pos >= input.len() {
        return Err(BencodeError::UnexpectedEof);
    }

    let len_str = std::str::from_utf8(&input[pos..colon_pos])
        .map_err(|_| BencodeError::InvalidLength(format!("{:?}", &input[pos..colon_pos])))?;
    let len: usize = len_str
        .parse()
        .map_err(|_| BencodeError::InvalidLength(len_str.to_string()))?;

    let start = colon_pos + 1;
    let end = start + len;

    if end > input.len() {
        return Err(BencodeError::UnexpectedEof);
    }

    Ok((Bencode::Bytes(input[start..end].to_vec()), end))
}

fn decode_list(input: &[u8], pos: usize) -> Result<(Bencode, usize), BencodeError> {
    let mut items = Vec::new();
    let mut current = pos + 1;

    while current < input.len() && input[current] != b'e' {
        let (item, next) = decode_inner(input, current)?;
        items.push(item);
        current = next;
    }

    if current >= input.len() {
        return Err(BencodeError::UnexpectedEof);
    }

    Ok((Bencode::List(items), current + 1))
}

fn decode_dict(input: &[u8], pos: usize) -> Result<(Bencode, usize), BencodeError> {
    let mut map = BTreeMap::new();
    let mut current = pos + 1;

    while current < input.len() && input[current] != b'e' {
        let (key_bencode, next) = decode_bytes(input, current)?;
        let key_bytes = match key_bencode {
            Bencode::Bytes(b) => b,
            _ => return Err(BencodeError::InvalidBytes),
        };
        let key = String::from_utf8(key_bytes).map_err(|_| BencodeError::InvalidBytes)?;

        if next >= input.len() {
            return Err(BencodeError::UnexpectedEof);
        }

        let (value, next) = decode_inner(input, next)?;
        map.insert(key, value);
        current = next;
    }

    if current >= input.len() {
        return Err(BencodeError::UnexpectedEof);
    }

    Ok((Bencode::Dict(map), current + 1))
}

impl Bencode {
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            Bencode::Integer(n) => Some(*n),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Bencode::Bytes(b) => Some(b),
            _ => None,
        }
    }

    pub fn as_string(&self) -> Option<String> {
        match self {
            Bencode::Bytes(b) => String::from_utf8(b.clone()).ok(),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&Vec<Bencode>> {
        match self {
            Bencode::List(l) => Some(l),
            _ => None,
        }
    }

    pub fn as_dict(&self) -> Option<&BTreeMap<String, Bencode>> {
        match self {
            Bencode::Dict(d) => Some(d),
            _ => None,
        }
    }

    pub fn get(&self, key: &str) -> Option<&Bencode> {
        self.as_dict()?.get(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── decode: Integer ──────────────────────────────────────────────

    #[test]
    fn test_integer() {
        assert_eq!(decode(b"i42e").unwrap(), Bencode::Integer(42));
        assert_eq!(decode(b"i-3e").unwrap(), Bencode::Integer(-3));
    }

    #[test]
    fn test_integer_zero() {
        assert_eq!(decode(b"i0e").unwrap(), Bencode::Integer(0));
    }

    #[test]
    fn test_integer_large() {
        assert_eq!(
            decode(b"i9999999999e").unwrap(),
            Bencode::Integer(9_999_999_999)
        );
    }

    #[test]
    fn test_integer_negative_large() {
        assert_eq!(
            decode(b"i-9999999999e").unwrap(),
            Bencode::Integer(-9_999_999_999)
        );
    }

    #[test]
    fn test_integer_i64_max() {
        assert_eq!(
            decode(b"i9223372036854775807e").unwrap(),
            Bencode::Integer(i64::MAX)
        );
    }

    #[test]
    fn test_integer_i64_min() {
        assert_eq!(
            decode(b"i-9223372036854775808e").unwrap(),
            Bencode::Integer(i64::MIN)
        );
    }

    #[test]
    fn test_integer_invalid_no_e() {
        assert!(matches!(decode(b"i42"), Err(BencodeError::UnexpectedEof)));
    }

    #[test]
    fn test_integer_invalid_chars() {
        assert!(matches!(
            decode(b"iabce"),
            Err(BencodeError::InvalidInteger(_))
        ));
    }

    #[test]
    fn test_integer_empty() {
        assert!(matches!(
            decode(b"ie"),
            Err(BencodeError::InvalidInteger(_))
        ));
    }

    // ── decode: Bytes ────────────────────────────────────────────────

    #[test]
    fn test_bytes() {
        assert_eq!(
            decode(b"5:hello").unwrap(),
            Bencode::Bytes(b"hello".to_vec())
        );
    }

    #[test]
    fn test_bytes_empty() {
        assert_eq!(decode(b"0:").unwrap(), Bencode::Bytes(Vec::new()));
    }

    #[test]
    fn test_bytes_binary() {
        let data = vec![0u8, 1, 255, 128, 64];
        let mut input = format!("{}:", data.len()).into_bytes();
        input.extend_from_slice(&data);
        assert_eq!(decode(&input).unwrap(), Bencode::Bytes(data));
    }

    #[test]
    fn test_bytes_truncated() {
        assert!(matches!(
            decode(b"10:hello"),
            Err(BencodeError::UnexpectedEof)
        ));
    }

    #[test]
    fn test_bytes_invalid_length() {
        assert!(matches!(
            decode(b"abc:hello"),
            Err(BencodeError::InvalidLength(_))
        ));
    }

    #[test]
    fn test_bytes_no_colon() {
        assert!(matches!(
            decode(b"5hello"),
            Err(BencodeError::UnexpectedEof)
        ));
    }

    // ── decode: List ─────────────────────────────────────────────────

    #[test]
    fn test_list() {
        let result = decode(b"li1ei2ei3ee").unwrap();
        assert_eq!(
            result,
            Bencode::List(vec![
                Bencode::Integer(1),
                Bencode::Integer(2),
                Bencode::Integer(3)
            ])
        );
    }

    #[test]
    fn test_list_empty() {
        assert_eq!(decode(b"le").unwrap(), Bencode::List(Vec::new()));
    }

    #[test]
    fn test_list_nested() {
        let result = decode(b"lli1eele").unwrap();
        assert_eq!(
            result,
            Bencode::List(vec![
                Bencode::List(vec![Bencode::Integer(1)]),
                Bencode::List(Vec::new())
            ])
        );
    }

    #[test]
    fn test_list_mixed_types() {
        let result = decode(b"li42e5:helloleee").unwrap();
        assert_eq!(
            result,
            Bencode::List(vec![
                Bencode::Integer(42),
                Bencode::Bytes(b"hello".to_vec()),
                Bencode::List(Vec::new())
            ])
        );
    }

    #[test]
    fn test_list_no_end() {
        assert!(matches!(decode(b"li1e"), Err(BencodeError::UnexpectedEof)));
    }

    // ── decode: Dict ─────────────────────────────────────────────────

    #[test]
    fn test_dict() {
        let result = decode(b"d3:cow3:moo4:spam3:egge").unwrap();
        let dict = result.as_dict().unwrap();
        assert_eq!(dict.get("cow").unwrap().as_string().unwrap(), "moo");
        assert_eq!(dict.get("spam").unwrap().as_string().unwrap(), "egg");
    }

    #[test]
    fn test_dict_empty() {
        assert_eq!(decode(b"de").unwrap(), Bencode::Dict(BTreeMap::new()));
    }

    #[test]
    fn test_dict_nested() {
        let result = decode(b"d5:innerd3:key5:valueee").unwrap();
        let outer = result.as_dict().unwrap();
        let inner = outer.get("inner").unwrap().as_dict().unwrap();
        assert_eq!(inner.get("key").unwrap().as_string().unwrap(), "value");
    }

    #[test]
    fn test_dict_no_end() {
        assert!(matches!(
            decode(b"d3:cow3:moo"),
            Err(BencodeError::UnexpectedEof)
        ));
    }

    #[test]
    fn test_dict_non_string_key() {
        // Dict keys must be strings (byte sequences starting with digit)
        assert!(matches!(
            decode(b"di42e3:fooe"),
            Err(BencodeError::UnexpectedChar('i'))
        ));
    }

    // ── decode: Error cases ──────────────────────────────────────────

    #[test]
    fn test_decode_empty_input() {
        assert!(matches!(decode(b""), Err(BencodeError::UnexpectedEof)));
    }

    #[test]
    fn test_decode_unexpected_char() {
        assert!(matches!(
            decode(b"x"),
            Err(BencodeError::UnexpectedChar('x'))
        ));
    }

    #[test]
    fn test_decode_leftover_bytes_ok() {
        // decode returns first value, ignoring trailing data
        let result = decode(b"i42e_extra").unwrap();
        assert_eq!(result, Bencode::Integer(42));
    }

    // ── encode ───────────────────────────────────────────────────────

    #[test]
    fn test_encode_integer() {
        assert_eq!(encode(&Bencode::Integer(42)), b"i42e");
        assert_eq!(encode(&Bencode::Integer(-3)), b"i-3e");
        assert_eq!(encode(&Bencode::Integer(0)), b"i0e");
    }

    #[test]
    fn test_encode_bytes() {
        assert_eq!(encode(&Bencode::Bytes(b"hello".to_vec())), b"5:hello");
    }

    #[test]
    fn test_encode_empty_bytes() {
        assert_eq!(encode(&Bencode::Bytes(Vec::new())), b"0:");
    }

    #[test]
    fn test_encode_list() {
        let val = Bencode::List(vec![Bencode::Integer(1), Bencode::Integer(2)]);
        assert_eq!(encode(&val), b"li1ei2ee");
    }

    #[test]
    fn test_encode_empty_list() {
        assert_eq!(encode(&Bencode::List(Vec::new())), b"le");
    }

    #[test]
    fn test_encode_dict() {
        let mut map = BTreeMap::new();
        map.insert("cow".to_string(), Bencode::Bytes(b"moo".to_vec()));
        map.insert("spam".to_string(), Bencode::Bytes(b"egg".to_vec()));
        let val = Bencode::Dict(map);
        let encoded = encode(&val);
        // BTreeMap sorts keys alphabetically
        assert_eq!(encoded, b"d3:cow3:moo4:spam3:egge");
    }

    #[test]
    fn test_encode_empty_dict() {
        assert_eq!(encode(&Bencode::Dict(BTreeMap::new())), b"de");
    }

    // ── roundtrip ────────────────────────────────────────────────────

    #[test]
    fn test_roundtrip_integer() {
        let val = Bencode::Integer(12345);
        assert_eq!(decode(&encode(&val)).unwrap(), val);
    }

    #[test]
    fn test_roundtrip_bytes() {
        let val = Bencode::Bytes(b"test data".to_vec());
        assert_eq!(decode(&encode(&val)).unwrap(), val);
    }

    #[test]
    fn test_roundtrip_complex() {
        let mut inner = BTreeMap::new();
        inner.insert("name".to_string(), Bencode::Bytes(b"test".to_vec()));
        inner.insert("size".to_string(), Bencode::Integer(1024));
        let val = Bencode::List(vec![
            Bencode::Integer(42),
            Bencode::Bytes(b"hello".to_vec()),
            Bencode::Dict(inner),
        ]);
        let encoded = encode(&val);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn test_roundtrip_nested_empty() {
        let val = Bencode::List(vec![
            Bencode::List(Vec::new()),
            Bencode::Dict(BTreeMap::new()),
            Bencode::Bytes(Vec::new()),
        ]);
        assert_eq!(decode(&encode(&val)).unwrap(), val);
    }

    // ── Bencode accessor methods ─────────────────────────────────────

    #[test]
    fn test_as_integer_some() {
        assert_eq!(Bencode::Integer(99).as_integer(), Some(99));
    }

    #[test]
    fn test_as_integer_none() {
        assert_eq!(Bencode::Bytes(b"x".to_vec()).as_integer(), None);
    }

    #[test]
    fn test_as_bytes_some() {
        assert_eq!(
            Bencode::Bytes(b"abc".to_vec()).as_bytes(),
            Some(b"abc".as_ref())
        );
    }

    #[test]
    fn test_as_bytes_none() {
        assert_eq!(Bencode::Integer(1).as_bytes(), None);
    }

    #[test]
    fn test_as_string_valid_utf8() {
        assert_eq!(
            Bencode::Bytes(b"hello".to_vec()).as_string(),
            Some("hello".to_string())
        );
    }

    #[test]
    fn test_as_string_invalid_utf8() {
        assert_eq!(Bencode::Bytes(vec![0xff, 0xfe]).as_string(), None);
    }

    #[test]
    fn test_as_string_non_bytes() {
        assert_eq!(Bencode::Integer(1).as_string(), None);
    }

    #[test]
    fn test_as_list_some() {
        let list = Bencode::List(vec![Bencode::Integer(1)]);
        assert!(list.as_list().is_some());
        assert_eq!(list.as_list().unwrap().len(), 1);
    }

    #[test]
    fn test_as_list_none() {
        assert_eq!(Bencode::Integer(1).as_list(), None);
    }

    #[test]
    fn test_as_dict_some() {
        let dict = Bencode::Dict(BTreeMap::new());
        assert!(dict.as_dict().is_some());
    }

    #[test]
    fn test_as_dict_none() {
        assert_eq!(Bencode::Integer(1).as_dict(), None);
    }

    #[test]
    fn test_get_existing_key() {
        let mut map = BTreeMap::new();
        map.insert("key".to_string(), Bencode::Integer(42));
        let dict = Bencode::Dict(map);
        assert_eq!(dict.get("key").unwrap().as_integer(), Some(42));
    }

    #[test]
    fn test_get_missing_key() {
        let dict = Bencode::Dict(BTreeMap::new());
        assert!(dict.get("missing").is_none());
    }

    #[test]
    fn test_get_on_non_dict() {
        assert!(Bencode::Integer(1).get("key").is_none());
        assert!(Bencode::List(vec![]).get("key").is_none());
        assert!(Bencode::Bytes(vec![]).get("key").is_none());
    }

    // ── BencodeError Display ─────────────────────────────────────────

    #[test]
    fn test_error_display_unexpected_eof() {
        let e = BencodeError::UnexpectedEof;
        assert_eq!(format!("{}", e), "unexpected end of input");
    }

    #[test]
    fn test_error_display_invalid_integer() {
        let e = BencodeError::InvalidInteger("abc".to_string());
        assert_eq!(format!("{}", e), "invalid integer: abc");
    }

    #[test]
    fn test_error_display_invalid_length() {
        let e = BencodeError::InvalidLength("xyz".to_string());
        assert_eq!(format!("{}", e), "invalid string length: xyz");
    }

    #[test]
    fn test_error_display_invalid_bytes() {
        let e = BencodeError::InvalidBytes;
        assert_eq!(format!("{}", e), "invalid byte sequence");
    }

    #[test]
    fn test_error_display_unexpected_char() {
        let e = BencodeError::UnexpectedChar('z');
        assert_eq!(format!("{}", e), "unexpected character: z");
    }

    #[test]
    fn test_error_display_invalid_format() {
        let e = BencodeError::InvalidFormat;
        assert_eq!(format!("{}", e), "invalid format");
    }

    // ── BencodeError Debug ───────────────────────────────────────────

    #[test]
    fn test_error_debug() {
        let e = BencodeError::UnexpectedEof;
        let dbg = format!("{:?}", e);
        assert!(dbg.contains("UnexpectedEof"));
    }

    // ── Clone/PartialEq ──────────────────────────────────────────────

    #[test]
    fn test_clone_integer() {
        let val = Bencode::Integer(42);
        assert_eq!(val.clone(), val);
    }

    #[test]
    fn test_clone_bytes() {
        let val = Bencode::Bytes(b"test".to_vec());
        assert_eq!(val.clone(), val);
    }

    #[test]
    fn test_clone_list() {
        let val = Bencode::List(vec![Bencode::Integer(1)]);
        assert_eq!(val.clone(), val);
    }

    #[test]
    fn test_clone_dict() {
        let mut map = BTreeMap::new();
        map.insert("k".to_string(), Bencode::Integer(1));
        let val = Bencode::Dict(map);
        assert_eq!(val.clone(), val);
    }

    #[test]
    fn test_debug_trait() {
        let val = Bencode::Integer(42);
        let dbg = format!("{:?}", val);
        assert!(dbg.contains("Integer"));
        assert!(dbg.contains("42"));
    }

    #[test]
    fn test_partial_eq_different_types() {
        assert_ne!(Bencode::Integer(1), Bencode::Bytes(b"1".to_vec()));
        assert_ne!(Bencode::List(vec![]), Bencode::Dict(BTreeMap::new()));
    }

    // ── Torrent-like structure ───────────────────────────────────────

    #[test]
    fn test_decode_torrent_like_structure() {
        // Simulates a minimal .torrent file structure
        let data = b"d8:announce35:http://tracker.example.com/announce4:infod6:lengthi1024e4:name8:test.txt12:piece lengthi16384e6:pieces20:aaaaaaaaaaaaaaaaaaaattee";
        let result = decode(data).unwrap();
        let dict = result.as_dict().unwrap();
        assert_eq!(
            dict.get("announce").unwrap().as_string().unwrap(),
            "http://tracker.example.com/announce"
        );
        let info = dict.get("info").unwrap().as_dict().unwrap();
        assert_eq!(info.get("length").unwrap().as_integer(), Some(1024));
        assert_eq!(info.get("name").unwrap().as_string().unwrap(), "test.txt");
        assert_eq!(info.get("piece length").unwrap().as_integer(), Some(16384));
        assert_eq!(info.get("pieces").unwrap().as_bytes().unwrap().len(), 20);
    }
}
