use crate::services::rpc::{HelloResponse, ReqId, Response};
use crate::services::transactor::backend::Backend;
use crate::services::transactor::methods::Method;
use crate::{Error, Result};
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt, TryStreamExt};
use futures::channel::{oneshot, mpsc};
use reqwest::Client;
use reqwest_websocket::{Message, RequestBuilderExt, WebSocket};
use serde::de::DeserializeOwned;
use serde::{Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use bytes::Bytes;
use futures::channel::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use tracing::{trace, warn};
use url::Url;
use tokio_with_wasm::alias as tokio;

enum Command {
    Call {
        payload: Value,
        reply_tx: oneshot::Sender<Result<Response<Value>>>,
        id: ReqId,
    },
    Close,
}

async fn socket_task(
    mut write: SplitSink<WebSocket, Message>,
    mut read: SplitStream<WebSocket>,
    mut cmd_rx: mpsc::UnboundedReceiver<Command>,
) -> Result<()> {
    let mut pending = HashMap::<ReqId, oneshot::Sender<Result<Response<Value>>>>::new();
    let mut binary_mode   = false;
    let mut use_compression = false;
    
    loop {
        tokio::select! {
            Some(cmd) = cmd_rx.next() => match cmd {
                Command::Call { id, payload, reply_tx } => {
                    pending.insert(id.clone(), reply_tx);
                    write.send(encode_message(&payload, binary_mode)?).await?;
                }
                Command::Close => break,
            },

            Some(message) = read.next() => {
                trace!(target: "ws", ?message, "Got message");

                let response: Response<Value>;
                let payload: Bytes;
                match message? {
                    Message::Text(resp) => {
                        response = serde_json::from_str(&resp)?;
                        payload = resp.into();
                    },
                    Message::Binary(resp) => {
                        response = serde_json::from_slice(&resp)?;
                        payload = resp;
                    },
                    Message::Ping(payload) => {
                        trace!(target: "ws", ?payload, "Received ping, replying...");
                        let payload = json!({
                            "method": Method::Ping.camel(),
                            "params": [],
                        });

                        write.send(encode_message(&payload, binary_mode)?).await?;
                        continue;
                    },
                    _ => continue,
                }
                
                if response.result.as_ref().is_some_and(|v| v == "ping") {
                    trace!(target: "ws", ?payload, "Received ping, replying...");
                    let payload = json!({
                        "method": Method::Ping.camel(),
                        "params": [],
                    });

                    write.send(encode_message(&payload, binary_mode)?).await?;
                    continue;
                }
                
                if matches!(response.id, Some(ReqId::Num(-1))) {
                    let hello = serde_json::from_slice::<HelloResponse>(&payload)?;
                    binary_mode = hello.binary;
                    use_compression = hello.use_compression.unwrap_or(false);
                    continue;
                }
                
                if let Some(id) = &response.id {
                    if let Some(tx) = pending.remove(id) {
                        let _ = tx.send(Ok(response)).ok();
                        continue;
                    }
                }
            }
        }
    }
    
    Ok(())
}

pub struct WsBackend {
    cmd_tx: mpsc::UnboundedSender<Command>,
    next_id: AtomicI32,
    base: Url,
    _handle: JoinHandle<()>,
}

impl WsBackend {
    pub(in crate::services::transactor) async fn connect(base: Url, token: &str) -> Result<Self> {
        let url = base.join(token)?;
        let resp = Client::default().get(url).bearer_auth(token).upgrade().send().await?;
        let ws = resp.into_websocket().await?;

        let (write, read) = ws.split();
        let (cmd_tx, cmd_rx) = mpsc::unbounded::<Command>();
        let handle = tokio::spawn(async move {
            if let Err(e) = socket_task(write, read, cmd_rx).await {
                warn!(target:"ws", ?e, "socket task crashed");
            }
        });
        
        Ok(Self { base, next_id: AtomicI32::new(1), cmd_tx, _handle: handle })
    }
}

fn encode_message<Q: Serialize>(value: &Q, binary_mode: bool) -> Result<Message> {
    if binary_mode {
        Ok(Message::Binary(serde_json::to_vec(value)?.into()))
    } else {
        Ok(Message::Text(serde_json::to_string(value)?))
    }
}

impl Backend for WsBackend {
    async fn get<T: DeserializeOwned + Send>(
        &mut self,
        method: Method,
        params: impl IntoIterator<Item = (&str, &str)>,
    ) -> Result<T> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed).into();
        
        let param_values = params.into_iter().map(|(_k, v)| v).collect::<Vec<_>>();
        
        let payload = json!({
            "method": method.camel(),
            "params": param_values,
        });

        send_and_wait(&mut self.cmd_tx, id, payload).await
    }

    async fn post<T: DeserializeOwned + Send, Q: Serialize>(&mut self, method: Method, body: &Q) -> Result<T> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed).into();
        
        let payload = json!({
            "method": method.camel(),
            "body": body,
        });

        send_and_wait(&mut self.cmd_tx, id, payload).await
    }

    fn base(&self) -> &Url {
        &self.base
    }
}

async fn send_and_wait<T: DeserializeOwned + Send>(cmd_tx: &mut UnboundedSender<Command>, id: ReqId, payload: Value) -> Result<T> {
    trace!(target: "ws", ?payload, "Sending message");

    let (reply_tx, reply_rx) = oneshot::channel();
    cmd_tx.send(Command::Call {
        payload,
        reply_tx,
        id,
    }).await.ok();

    let Ok(reply) = reply_rx.await else {
        return Err(Error::Other("connection closed before reply"));
    };
    
    let reply = reply?;
    let Some(result) = reply.result else {
        return Err(Error::Other("server didn't return a result"));
    };

    serde_json::from_value(result).map_err(|e| e.into())
}