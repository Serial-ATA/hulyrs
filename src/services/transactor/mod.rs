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
use crate::services::transactor::document::FindOptions;
use crate::services::transactor::methods::Method;
use secrecy::{ExposeSecret, SecretString};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use subscription::SubscribedQuery;
use url::Url;

pub mod backend;
pub mod document;
pub mod event;
pub mod methods;
pub mod person;
mod subscription;

struct TransactorClientInner<B> {
    pub workspace: WorkspaceUuid,
    token: SecretString,
    backend: B,
}

#[derive(Clone)]
pub struct TransactorClient<B> {
    backend: B,
}

impl<B: Backend> PartialEq for TransactorClient<B> {
    fn eq(&self, other: &Self) -> bool {
        self.backend.workspace() == other.backend.workspace()
            && self.backend.provide_token() == other.backend.provide_token()
            && self.base() == other.base()
    }
}

impl<B: Backend> super::TokenProvider for &TransactorClient<B> {
    fn provide_token(&self) -> Option<&str> {
        self.backend.provide_token()
    }
}

impl<B: Backend> TransactorClient<B> {
    pub fn base(&self) -> &Url {
        self.backend.base()
    }

    pub async fn get<'a, T: DeserializeOwned + Send>(
        &self,
        method: Method,
        params: impl IntoIterator<Item = (&'a str, Value)> + Send,
    ) -> Result<T> {
        self.backend.get(method, params).await
    }

    pub async fn post<T: DeserializeOwned + Send, Q: Serialize>(
        &self,
        method: Method,
        body: &Q,
    ) -> Result<T> {
        self.backend.post(method, body).await
    }

    pub(in crate::services::transactor) fn backend(&self) -> &B {
        &self.backend
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
        Ok(Self {
            backend: HttpBackend::new(http, base, workspace, token),
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
        let backend = WsBackend::connect(base, workspace, token.expose_secret(), opts).await?;

        Ok(Self { backend })
    }

    pub async fn subscribe<Q: Serialize + Clone, T: DeserializeOwned>(
        &self,
        class: impl AsRef<str>,
        query: Q,
        options: FindOptions,
    ) -> SubscribedQuery<Q, T> {
        SubscribedQuery::new(self.clone(), class.as_ref(), query, options)
    }
}
