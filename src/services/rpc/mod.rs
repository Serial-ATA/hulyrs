use serde::{Deserialize, Serialize};
use crate::services::core::Account;
use crate::services::Status;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum ReqId {
    Str(String),
    Num(i32),
}

impl From<String> for ReqId {
    fn from(s: String) -> Self {
        ReqId::Str(s)
    }
}

impl From<i32> for ReqId {
    fn from(i: i32) -> Self {
        ReqId::Num(i)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RateLimitInfo {
    pub remaining: u32,
    pub limit: u32,
    pub current: u32,
    pub reset: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after: Option<u32>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Chunk {
    pub index: u32,
    pub r#final: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Response<R> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<R>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<ReqId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<Status>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminate: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit: Option<RateLimitInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunk: Option<Chunk>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bfst: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue: Option<u32>
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct HelloResponse {
    #[serde(flatten)]
    pub response: Response<String>,
    pub binary: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reconnect: Option<bool>,
    pub server_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_tx: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_hash: Option<String>,
    pub account: Account,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_compression: Option<bool>
}