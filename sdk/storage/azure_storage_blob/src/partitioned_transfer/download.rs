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

    let partition_size = partition_size as u64;
    let total_ranges =
        ((content_range.total_length() as f64) / (partition_size as f64)).ceil() as u64;

    let mut ranges: VecDeque<Range<u64>> = (1u64..total_ranges)
        .map(|i| (i * partition_size..i * partition_size + partition_size))
        .collect();
    let mut ops = VecDeque::from([TransferOp::Transferring(Box::pin(
        initial_response.into_raw_body().collect(),
    ))]);

    let stream = futures::stream::poll_fn(move |cx| {
        while ops.len() < parallel {
            match ranges.pop_front() {
                Some(range) => ops.push_back(TransferOp::AwaitingResponse(Box::pin(
                    client.transfer_range(range),
                ))),
                None => break,
            }
        }

        for op in ops.iter_mut() {
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
        match ops.pop_front() {
            Some(TransferOp::Ready(bytes)) => Poll::Ready(Some(Ok(bytes))),
            Some(transfer_op) => {
                ops.push_front(transfer_op);
                Poll::Pending
            }
            None => Poll::Ready(None),
        }
    });

    Ok(Box::new(stream))
}

enum TransferOp<'a> {
    AwaitingResponse(Pin<Box<dyn Future<Output = AzureResult<Response<()>>> + 'a>>),
    Transferring(Pin<Box<dyn Future<Output = AzureResult<Bytes>> + 'a>>),
    Ready(Bytes),
}
