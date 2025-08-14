use std::{mem, pin::Pin, task::Poll};

use bytes::Bytes;
use futures::{
    ready,
    stream::{Fuse, FusedStream},
    Stream, StreamExt,
};

pub(crate) struct PartitionedStream<TInner: Stream<Item = azure_core::Result<Bytes>>> {
    inner_stream: Fuse<TInner>,
    bufs: Vec<Bytes>,
    partition_len: usize,
}

impl<S: Stream<Item = azure_core::Result<Bytes>>> PartitionedStream<S> {
    pub(crate) fn new(inner: S, partition_len: usize) -> Self {
        assert!(partition_len > 0);
        Self {
            inner_stream: inner.fuse(),
            bufs: Vec::new(),
            partition_len,
        }
    }
}

impl<TInner: Stream<Item = azure_core::Result<Bytes>> + Unpin> Stream
    for PartitionedStream<TInner>
{
    type Item = azure_core::Result<Vec<Bytes>>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            match ready!(this.inner_stream.poll_next_unpin(cx)) {
                Some(bytes) => {
                    this.bufs.push(bytes?);
                    if this.bufs.iter().map(|b| b.len()).sum::<usize>() >= this.partition_len {
                        return Poll::Ready(Some(Ok(mem::replace(&mut this.bufs, Vec::new()))));
                    }
                }
                None => {
                    return Poll::Ready(if this.bufs.is_empty() {
                        None
                    } else {
                        Some(Ok(mem::take(&mut this.bufs)))
                    })
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner_stream.size_hint()
    }
}

impl<TInner: FusedStream + Stream<Item = azure_core::Result<Bytes>> + Unpin> FusedStream
    for PartitionedStream<TInner>
{
    fn is_terminated(&self) -> bool {
        self.inner_stream.is_terminated() && self.bufs.is_empty()
    }
}
