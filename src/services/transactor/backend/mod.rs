use crate::Result;
use crate::services::transactor::methods::Method;
use serde::de::DeserializeOwned;
use serde::Serialize;
use url::Url;

pub mod http;
pub mod ws;

pub trait Backend {
    async fn get<T: DeserializeOwned + Send>(
        &mut self,
        method: Method,
        params: impl IntoIterator<Item = (&str, &str)>,
    ) -> Result<T>;

    async fn post<T: DeserializeOwned + Send, Q: Serialize>(
        &mut self,
        method: Method,
        body: &Q,
    ) -> Result<T>;
    
    fn base(&self) -> &Url;
}
