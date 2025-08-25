use std::future;

use azure_core::{
    http::Body,
    stream::{BytesStream, SeekableStream},
};
use bytes::Bytes;
use futures::StreamExt;

use crate::streams::partitioned_stream::PartitionedStream;

use super::*;

pub(crate) trait PartitionedUploadBehavior {
    async fn transfer_oneshot(&self, content: Body) -> AzureResult<()>;
    async fn transfer_partition(&self, offset: usize, content: Body) -> AzureResult<()>;
    async fn initialize(&self, content_len: usize) -> AzureResult<()>;
    async fn finalize(&self) -> AzureResult<()>;
}

pub(crate) async fn upload(
    content: Body,
    parallel: usize,
    partition_size: usize,
    client: &impl PartitionedUploadBehavior,
) -> AzureResult<()> {
    if content.len() <= partition_size {
        client.transfer_oneshot(content).await?;
        return Ok(());
    }

    client.initialize(content.len()).await?;

    match content {
        Body::Bytes(bytes) => {
            upload_bytes_partitions(bytes, parallel, partition_size, client).await?;
        }
        Body::SeekableStream(seekable_stream) => {
            upload_stream_partitions(seekable_stream, parallel, partition_size, client).await?;
        }
    }

    client.finalize().await?;

    Ok(())
}

async fn upload_bytes_partitions(
    content: Bytes,
    parallel: usize,
    partition_size: usize,
    client: &impl PartitionedUploadBehavior,
) -> AzureResult<()> {
    let num_partitions = div_round_up(content.len(), partition_size);
    let partitions = (0..num_partitions)
        .map(|i| i * partition_size)
        .map(|offset| offset..std::cmp::min(offset + partition_size, content.len()))
        .map(|range| (range.start, content.slice(range)));
    let ops = partitions
        .map(|(offset, bytes)| Ok(move || client.transfer_partition(offset, Body::Bytes(bytes))));
    run_all_with_concurrency_limit(futures::stream::iter(ops), parallel).await?;
    Ok(())
}

async fn upload_stream_partitions(
    content: Box<dyn SeekableStream>,
    parallel: usize,
    partition_size: usize,
    client: &impl PartitionedUploadBehavior,
) -> AzureResult<()> {
    let partitions = PartitionedStream::new(content, partition_size)
        .map_ok(BytesStream::new)
        .scan(0, |enumerated, result| match result {
            Ok(seekable_stream) => {
                let offset = *enumerated;
                *enumerated += seekable_stream.len();
                future::ready(Some(Ok((offset, seekable_stream))))
            }
            Err(e) => future::ready(Some(Err(e))),
        });
    let ops = partitions.map_ok(|(offset, stream)| {
        move || client.transfer_partition(offset, Body::SeekableStream(Box::new(stream)))
    });
    run_all_with_concurrency_limit(ops, parallel).await?;
    Ok(())
}
