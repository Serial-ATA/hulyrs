use crate::Result;
use crate::services::transactor::methods::Method;
use crate::services::{JsonClient, TokenProvider};
use reqwest_middleware::ClientWithMiddleware;
use secrecy::{ExposeSecret, SecretString};
use serde::Serialize;
use serde::de::DeserializeOwned;
use url::Url;

pub type HttpClient = ClientWithMiddleware;

pub struct HttpBackend {
    pub(in crate::services::transactor) base: Url,
    pub(in crate::services::transactor) client: HttpClient,
    pub(in crate::services::transactor) token: SecretString,
}

impl TokenProvider for &'_ HttpBackend {
    fn provide_token(&self) -> Option<&str> {
        Some(self.token.expose_secret())
    }
}

impl super::Backend for HttpBackend {
    async fn get<T: DeserializeOwned + Send>(
        &mut self,
        method: Method,
        params: impl IntoIterator<Item = (&str, &str)>,
    ) -> Result<T> {
        let mut url = self.base.join(&format!("/api/v1/{}", method.kebab()))?;
        let mut qp = url.query_pairs_mut();
        for (name, value) in params {
            qp.append_pair(name, value);
        }
        drop(qp);

        <crate::services::HttpClient as JsonClient>::get(&self.client, &*self, url).await
    }

    async fn post<T: DeserializeOwned + Send, Q: Serialize>(
        &mut self,
        method: Method,
        body: &Q,
    ) -> Result<T> {
        let url = self.base.join(&format!("/api/v1/{}", method.kebab()))?;
        <crate::services::HttpClient as JsonClient>::post(&self.client, &*self, url, body).await
    }

    fn base(&self) -> &Url {
        &self.base
    }
}
