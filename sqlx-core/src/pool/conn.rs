use crate::{Connection, Connect};
use std::sync::Arc;
use std::time::Instant;
use std::ops::{Deref, DerefMut};
use futures_util::future::BoxFuture;
use std::mem;

use super::inner::SharedPool;
use super::size::{DecreaseOnDrop, PoolSize};

pub struct PoolConnection<C>
    where
        C: Connection + Connect<Connection = C>,
{
    live: Option<Live<C>>,
    pool: Arc<SharedPool<C>>,
}

struct Live<C> {
    raw: C,
    created: Instant,
}

pub(super) struct Idle<C> {
    live: Live<C>,
    pub(super) since: Instant,
}

pub(super) struct Floating<'g, C> {
    live: Live<C>,
    guard: DecreaseOnDrop<'g>,
}

const DEREF_ERR: &str = "(bug) connection already released to pool";

impl<C> Deref for PoolConnection<C>
    where
        C: Connection + Connect<Connection = C>,
{
    type Target = C;

    fn deref(&self) -> &Self::Target {
        &self.live.as_ref().expect(DEREF_ERR).raw
    }
}

impl<C> DerefMut for PoolConnection<C>
    where
        C: Connection + Connect<Connection = C>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.live.as_mut().expect(DEREF_ERR).raw
    }
}

impl<C> Connection for PoolConnection<C>
    where
        C: Connection + Connect<Connection = C>,
{
    fn close(mut self) -> BoxFuture<'static, crate::Result<()>> {
        Box::pin(async move {
            if let Some(live) = self.live.take() {
                self.pool.size.decrease_on_drop();
                let raw = live.raw;

                // Explicitly close the connection
                raw.close().await?;
            }

            Ok(())
        })
    }
}

impl<C> Drop for PoolConnection<C>
    where
        C: Connection + Connect<Connection = C>,
{
    fn drop(&mut self) {
        if let Some(live) = self.live.take() {
            self.pool.release(live);
        }
    }
}

impl Floating<'_, C> {
    pub fn attach(self, pool: &Arc<SharedPool<C>>) -> PoolConnection<C> {
        let Floating { live, guard } = self;
        guard.cancel();
        PoolConnection {
            live: Some(live),
            pool: Arc::clone(pool),
        }
    }
}

impl<C> Deref for Floating<'_, C> {
    type Target = C;

    fn deref(&self) -> &Self::Target {
        &self.live.raw
    }
}

impl<C> DerefMut for Floating<'_, C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.live.raw
    }
}
