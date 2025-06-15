//
// Copyright © 2025 Hardcore Engineering Inc.
//
// Licensed under the Eclipse Public License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License. You may
// obtain a copy of the License at https://www.eclipse.org/legal/epl-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//
// See the License for the specific language governing permissions and
// limitations under the License.
//

use crate::Result;
use crate::services::ForceScheme;
use crate::services::core::WorkspaceUuid;
use crate::services::transactor::backend::Backend;
use crate::services::transactor::backend::http::{HttpBackend, HttpClient};
use crate::services::transactor::backend::ws::{WsBackend, WsBackendOpts};
use crate::services::transactor::methods::Method;
use reqwest_websocket::{Message, RequestBuilderExt, WebSocket};
use secrecy::{ExposeSecret, SecretString};
use serde::Serialize;
use serde::de::DeserializeOwned;
use url::Url;

pub mod backend;
pub mod document;
pub mod event;
pub mod methods;
pub mod person;

pub struct TransactorRequest {
    pub method: String,
    pub params: Vec<(String, String)>,
}

#[derive(Clone)]
pub struct TransactorClient<B> {
    pub workspace: WorkspaceUuid,
    token: SecretString,
    backend: B,
}

impl<B: Backend> PartialEq for TransactorClient<B> {
    fn eq(&self, other: &Self) -> bool {
        self.workspace == other.workspace
            && self.token.expose_secret() == other.token.expose_secret()
            && self.base() == other.base()
    }
}

impl<B> super::TokenProvider for &TransactorClient<B> {
    fn provide_token(&self) -> Option<&str> {
        Some(self.token.expose_secret())
    }
}

impl<B: Backend> TransactorClient<B> {
    pub fn base(&self) -> &Url {
        self.backend.base()
    }

    pub async fn get<T: DeserializeOwned + Send>(
        &mut self,
        method: Method,
        params: impl IntoIterator<Item = (&str, &str)>,
    ) -> Result<T> {
        self.backend.get(method, params).await
    }

    pub async fn post<T: DeserializeOwned + Send, Q: Serialize>(
        &mut self,
        method: Method,
        body: &Q,
    ) -> Result<T> {
        self.backend.post(method, body).await
    }
}

impl TransactorClient<HttpBackend> {
    pub fn new(
        http: HttpClient,
        base: Url,
        workspace: WorkspaceUuid,
        token: impl Into<SecretString>,
    ) -> Result<Self> {
        let base = base.force_http_scheme();
        let token = token.into();
        Ok(Self {
            workspace,
            token: token.clone(),
            backend: HttpBackend {
                base,
                client: http,
                token,
            },
        })
    }
}

impl TransactorClient<WsBackend> {
    pub async fn new_ws(
        base: Url,
        workspace: WorkspaceUuid,
        token: impl Into<SecretString>,
        opts: WsBackendOpts,
    ) -> Result<Self> {
        let base = base.force_ws_scheme();
        let token = token.into();
        let backend = WsBackend::connect(base, token.expose_secret(), opts).await?;

        Ok(Self {
            workspace,
            token,
            backend,
        })
    }
}
