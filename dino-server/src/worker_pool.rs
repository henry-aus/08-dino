use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{mpsc::Receiver, Mutex};

use tokio::sync::oneshot::Sender;

use crate::{AppError, JsWorker, Req, Res};

use anyhow::{anyhow, Result};

pub struct Command {
    code: String,
    handler: String,
    req: Req,
    tx: Sender<Res>,
}

impl Command {
    pub fn new(code: String, handler: String, req: Req, tx: Sender<Res>) -> Self {
        Self {
            code,
            handler,
            req,
            tx,
        }
    }
}

pub struct WorkerPool {
    inner: Arc<Mutex<WorkerPoolInner>>,
}

struct WorkerPoolInner {
    registry: DashMap<String, JsWorker>,
    rx: Receiver<Command>,
}

unsafe impl Send for WorkerPoolInner {}

impl WorkerPoolInner {
    fn new(rx: Receiver<Command>) -> Self {
        WorkerPoolInner {
            registry: DashMap::new(),
            rx,
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        while let Some(cmd) = self.rx.recv().await {
            if self.registry.get(&cmd.code).is_none() {
                let worker = JsWorker::try_new(&cmd.code)?;
                self.registry.insert(cmd.code.clone(), worker);
            }

            let res = self
                .registry
                .get(&cmd.code)
                .ok_or(AppError::Anyhow(anyhow!("Can't find worker")))?
                .run(&cmd.handler, cmd.req)?;

            cmd.tx
                .send(res)
                .map_err(|_| AppError::Anyhow(anyhow!("Send back response failed")))?;
        }
        Ok(())
    }
}

impl WorkerPool {
    pub fn new(rx: Receiver<Command>) -> Self {
        WorkerPool {
            inner: Arc::new(Mutex::new(WorkerPoolInner::new(rx))),
        }
    }

    pub async fn start(&self) -> Result<()> {
        self.inner.lock().await.start().await
    }
}
