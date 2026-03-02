use std::{collections::VecDeque, num::NonZero, pin::Pin, task::Poll};

use futures::{stream::Fuse, Stream, StreamExt};
use pin_project::pin_project;

/// Similar to FuturesOrdered but for Streams.
///
/// This stream polls multiple streams at once, buffering each item in order per-stream.
/// It yields items flattened into a single stream, where all of the first stream's elements
/// will be yielded in order until stream completion, followed by all of the second stream's
/// elements in order until its stream completion, etc. until all streams have been consumed.
///
/// This type limits the number of streams being polled simultaneously, preferring streams at
/// the front of the queue. Only the first n streams will be polled and have their elements
/// buffered.
#[pin_project]
pub(crate) struct ParallelOrderedStream<I> {
    active_streams: VecDeque<Pin<Box<dyn BufferingStream<Item = I> + Send + Unpin>>>,
    pending_streams: VecDeque<Pin<Box<dyn Stream<Item = I> + Send + Unpin>>>,
    max_active_streams: usize,
}

impl<I: 'static> ParallelOrderedStream<I> {
    pub fn new(
        streams: impl IntoIterator<Item = Pin<Box<dyn Stream<Item = I> + Send + Unpin>>>,
        parallel: NonZero<usize>,
    ) -> Self {
        Self {
            active_streams: VecDeque::new(),
            pending_streams: streams.into_iter().collect(),
            max_active_streams: parallel.get(),
        }
    }
}

impl<I> Stream for ParallelOrderedStream<I>
where
    I: Send + Sync + 'static,
{
    type Item = I;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.project();

        loop {
            while this.active_streams.len() < *this.max_active_streams {
                match this.pending_streams.pop_front() {
                    Some(stream) => this
                        .active_streams
                        .push_back(Box::pin(BufferingStreamImpl::new(stream))),
                    None => break,
                }
            }

            for sub_stream in this.active_streams.iter_mut().skip(1) {
                sub_stream.poll_to_buffer(cx);
            }

            match this.active_streams.front_mut() {
                Some(front_stream) => match front_stream.poll_next_unpin(cx) {
                    // if front stream is finished, remove it and loop back around to refill and poll next
                    Poll::Ready(None) => {
                        this.active_streams.pop_front();
                    }
                    Poll::Ready(Some(item)) => return Poll::Ready(Some(item)),
                    Poll::Pending => return Poll::Pending,
                },
                None => {
                    // if there are also no pending streams, return stream completion
                    // otherwise, loop back around to refill active streams
                    if this.pending_streams.is_empty() {
                        return Poll::Ready(None);
                    }
                }
            }
        }
    }
}

trait BufferingStream: Stream + Send + Unpin {
    fn items_buffered(&self) -> usize;
    fn poll_to_buffer(&mut self, cx: &mut std::task::Context<'_>);
}

#[pin_project]
struct BufferingStreamImpl<St>
where
    St: Stream,
{
    stream: Fuse<St>,
    buffer: VecDeque<St::Item>,
}

impl<St> BufferingStreamImpl<St>
where
    St: Stream,
{
    fn new(stream: St) -> Self {
        Self {
            stream: stream.fuse(),
            buffer: VecDeque::new(),
        }
    }
}

impl<St> BufferingStream for BufferingStreamImpl<St>
where
    St: Stream + Unpin + Send,
    St::Item: Send,
{
    fn items_buffered(&self) -> usize {
        self.buffer.len()
    }

    fn poll_to_buffer(&mut self, cx: &mut std::task::Context<'_>) {
        if let Poll::Ready(Some(item)) = self.stream.poll_next_unpin(cx) {
            self.buffer.push_back(item);
        }
    }
}

impl<St> Stream for BufferingStreamImpl<St>
where
    St: Stream + Unpin + Send,
    St::Item: Send,
{
    type Item = St::Item;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.as_mut().poll_to_buffer(cx);
        let this = self.project();
        match this.buffer.pop_front() {
            Some(item) => Poll::Ready(Some(item)),
            None => {
                if this.stream.is_done() {
                    Poll::Ready(None)
                } else {
                    Poll::Pending
                }
            }
        }
    }
}
