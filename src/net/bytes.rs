use crate::{net::*, *};

impl State {
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_lexpr::to_vec(self).expect("serialization error")
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ParseError> {
        serde_lexpr::from_slice(bytes).map_err(Into::into)
    }
}

impl std::str::FromStr for Request {
    type Err = serde_lexpr::error::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_lexpr::from_str::<Request>(s)
    }
}

pub struct Parser<R>(serde_lexpr::parse::Parser<R>);
pub enum ParseError {
    P(serde_lexpr::parse::Error),
    B(serde_lexpr::Error),
}

impl From<serde_lexpr::parse::Error> for ParseError {
    fn from(e: serde_lexpr::parse::Error) -> Self {
        ParseError::P(e)
    }
}

impl From<serde_lexpr::Error> for ParseError {
    fn from(e: serde_lexpr::Error) -> Self {
        ParseError::B(e)
    }
}

impl From<ParseError> for io::Error {
    fn from(e: ParseError) -> Self {
        match e {
            ParseError::B(e) => e.into(),
            ParseError::P(e) => e.into(),
        }
    }
}

impl<R> Parser<serde_lexpr::parse::IoRead<R>>
where
    R: io::Read,
{
    #[expect(unused)]
    pub fn from_reader(reader: R) -> Self {
        Self(serde_lexpr::parse::Parser::from_reader(reader))
    }

    #[expect(unused)]
    pub fn iter<T>(&mut self) -> impl Iterator<Item = Result<T, ParseError>>
    where
        T: for<'a> Deserialize<'a>,
    {
        self.0.value_iter().map(|v| {
            v.map_err(ParseError::from)
                .and_then(|v| serde_lexpr::from_value::<T>(&v).map_err(Into::into))
        })
    }

    #[expect(unused)]
    pub fn parse<T>(&mut self) -> Result<T, ParseError>
    where
        T: for<'a> Deserialize<'a>,
    {
        let v = self.0.expect_value()?;
        serde_lexpr::from_value(&v).map_err(Into::into)
    }
}

impl Response {
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_lexpr::to_vec(self).expect("serialization error")
    }
}
