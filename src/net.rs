use serde::{Deserialize, Serialize};

pub mod bytes;

#[derive(Debug, Deserialize, Serialize)]
pub enum Request {}
