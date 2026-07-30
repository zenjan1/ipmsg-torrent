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
}

pub fn decode(input: &[u8]) -> Result<Bencode, BencodeError> {
    let (value, _) = decode_inner(input, 0)?;
    Ok(value)
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

    #[test]
    fn test_integer() {
        assert_eq!(decode(b"i42e").unwrap(), Bencode::Integer(42));
        assert_eq!(decode(b"i-3e").unwrap(), Bencode::Integer(-3));
    }

    #[test]
    fn test_bytes() {
        assert_eq!(
            decode(b"5:hello").unwrap(),
            Bencode::Bytes(b"hello".to_vec())
        );
    }

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
    fn test_dict() {
        let result = decode(b"d3:cow3:moo4:spam3:egge").unwrap();
        let dict = result.as_dict().unwrap();
        assert_eq!(dict.get("cow").unwrap().as_string().unwrap(), "moo");
        assert_eq!(dict.get("spam").unwrap().as_string().unwrap(), "egg");
    }
}
