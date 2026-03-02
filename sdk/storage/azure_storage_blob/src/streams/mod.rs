use std::{future::Future, task::ready};

use futures::{stream::FusedStream, FutureExt, Stream, StreamExt};
use pin_project::pin_project;

pub(crate) mod parallel_ordered_stream;
pub(crate) mod partitioned_stream;

/// A stream that encapsulates the initial fetching of the underlying stream
/// within the stream polling interface.
///
/// When the internal state of this stream is a future to fetch the eventual
/// underlying stream, polling this stream polls that future until the future
/// completes, at which point the state becomes the underlying stream.
/// When the internal state of this stream is the underlying stream, polling
/// this stream polls the underlying stream.
#[pin_project]
pub(crate) struct SelfFetchingStream<St, F> {
    state: SelfFetchingStreamState<St, F>,
}

enum SelfFetchingStreamState<St, F> {
    Fetching(F),
    Stream(St),
}

impl<St, F, T, E> SelfFetchingStream<St, F>
where
    St: Stream<Item = Result<T, E>> + Send,
    F: Future<Output = Result<St, E>> + Send,
{
    pub(crate) fn new(future: F) -> Self {
        Self {
            state: SelfFetchingStreamState::Fetching(future),
        }
    }
}

impl<St, F, T, E> Stream for SelfFetchingStream<St, F>
where
    St: Stream<Item = Result<T, E>> + Send + Unpin,
    F: Future<Output = Result<St, E>> + Send + Unpin,
{
    type Item = St::Item;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        match this.state {
            SelfFetchingStreamState::Fetching(ref mut future) => {
                let mut stream = ready!(future.poll_unpin(cx))?;
                let poll = stream.poll_next_unpin(cx);
                *this.state = SelfFetchingStreamState::Stream(stream);
                poll
            }
            SelfFetchingStreamState::Stream(stream) => stream.poll_next_unpin(cx),
        }
    }
}

impl<St, F, T, E> FusedStream for SelfFetchingStream<St, F>
where
    St: FusedStream<Item = Result<T, E>> + Send + Unpin,
    F: Future<Output = Result<St, E>> + Send + Unpin,
{
    fn is_terminated(&self) -> bool {
        match self.state {
            SelfFetchingStreamState::Fetching(_) => false,
            SelfFetchingStreamState::Stream(ref stream) => stream.is_terminated(),
        }
    }
}
