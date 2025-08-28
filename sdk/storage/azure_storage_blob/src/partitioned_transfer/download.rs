use std::{collections::VecDeque, f32::consts::E, ops::Range, pin::Pin, task::Poll};

use azure_core::{
    http::{request::options::ContentRange, Response},
    Error,
};
use bytes::Bytes;
use futures::{
    future::{self, SelectAll},
    ready, FutureExt,
};

use super::*;

pub(crate) trait PartitionedDownloadBehavior {
    async fn transfer_range(&self, range: Range<u64>) -> AzureResult<Response<()>>;
}

pub(crate) async fn download<'a>(
    parallel: usize,
    partition_size: usize,
    client: &'a impl PartitionedDownloadBehavior,
) -> AzureResult<Box<dyn Stream<Item = AzureResult<Bytes>> + 'a>> {
    let err = || {
        azure_core::Error::message(
            azure_core::error::ErrorKind::Other,
            "Failed to parse Content-Range header {}.",
        )
    };
    let initial_response = client.transfer_range(0..partition_size as u64).await?;
    let content_range: ContentRange = initial_response
        .headers()
        .get_optional_as(&"content-range".into())?
        .ok_or_else(err)?;
    if content_range.end() + 1 == content_range.total_length() {
        return Ok(Box::new(initial_response.into_raw_body()));
    }

    let total_ranges = div_round_up(
        content_range.total_length().try_into().unwrap(),
        partition_size,
    );
    let ranges = (1..total_ranges).map(|i| i * partition_size);

    Ok(Box::new(ParallelSourceBufferedStream {
        client,
        ops: VecDeque::from([TransferOp::Transferring(Box::pin(
            initial_response.into_raw_body().collect(),
        ))]),
        max_ops: parallel,
        ranges: VecDeque::new(),
    }))
}

enum TransferOp<'a> {
    AwaitingResponse(Pin<Box<dyn Future<Output = AzureResult<Response<()>>> + 'a>>),
    Transferring(Pin<Box<dyn Future<Output = AzureResult<Bytes>> + 'a>>),
    Ready(Bytes),
}

struct ParallelSourceBufferedStream<'a, TClient: PartitionedDownloadBehavior> {
    client: &'a TClient,
    ops: VecDeque<TransferOp<'a>>,
    max_ops: usize,
    ranges: VecDeque<Range<u64>>,
}

impl<TClient: PartitionedDownloadBehavior> ParallelSourceBufferedStream<'_, TClient> {
    fn fill_ops(&mut self) {
        while self.ops.len() < self.max_ops {
            match self.ranges.pop_front() {
                Some(range) => self.ops.push_back(TransferOp::AwaitingResponse(Box::pin(
                    self.client.transfer_range(range),
                ))),
                None => break,
            }
        }
    }
}

impl<TClient: PartitionedDownloadBehavior> Stream for ParallelSourceBufferedStream<'_, TClient> {
    type Item = AzureResult<Bytes>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();

        this.fill_ops();

        for op in this.ops.iter_mut() {
            match op {
                TransferOp::AwaitingResponse(fut) => {
                    if let Poll::Ready(res) = fut.as_mut().poll(cx) {
                        match res {
                            Ok(response) => {
                                *op = TransferOp::Transferring(Box::pin(
                                    response.into_raw_body().collect(),
                                ));
                            }
                            Err(e) => return Poll::Ready(Some(Err(e))),
                        }
                    }
                }
                TransferOp::Transferring(fut) => {
                    if let Poll::Ready(res) = fut.as_mut().poll(cx) {
                        match res {
                            Ok(bytes) => *op = TransferOp::Ready(bytes),
                            Err(e) => return Poll::Ready(Some(Err(e))),
                        }
                    }
                }
                TransferOp::Ready(_bytes) => {}
            }
        }
        match this.ops.pop_front() {
            Some(TransferOp::Ready(bytes)) => Poll::Ready(Some(Ok(bytes))),
            Some(transfer_op) => {
                this.ops.push_front(transfer_op);
                Poll::Pending
            }
            None => Poll::Ready(None),
        }
    }
}
