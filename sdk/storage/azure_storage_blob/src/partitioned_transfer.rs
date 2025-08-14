use std::future::Future;

use azure_core::{
    http::{Body, RequestContent},
    stream::SeekableStream,
};
use bytes::{Buf, Bytes};
use futures::{AsyncReadExt, Stream, StreamExt, TryStreamExt};

use crate::streams::{
    multi_bytes_stream::MultiBytesStream,
    partitioned_stream::{self, PartitionedStream},
};

trait PartitionedUploadBehavior {
    async fn transfer_oneshot(&self, content: Body) -> azure_core::Result<()>;
    async fn transfer_partition(&self, content: Body) -> azure_core::Result<()>;
    async fn initialize(&self, content_len: usize) -> azure_core::Result<()>;
    async fn finalize(&self) -> azure_core::Result<()>;
}

enum ConcurrentAccessStrategy {
    None,
    ETagLock,
    Lease(String),
}

async fn upload(
    content: Body,
    parallel: usize,
    partition_size: usize,
    client: &impl PartitionedUploadBehavior,
) -> azure_core::Result<()> {
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
) -> azure_core::Result<()> {
    let num_partitions = div_round_up(content.len(), partition_size);
    let partitions = (0..num_partitions)
        .map(|i| i * partition_size)
        .map(|offset| offset..std::cmp::min(offset + partition_size, content.len()))
        .map(|range| content.slice(range));
    let ops = partitions.map(|bytes| || client.transfer_partition(Body::Bytes(bytes)));
    run_all_with_concurrency_limit(futures::stream::iter(ops), parallel).await?;
    Ok(())
}

async fn upload_stream_partitions(
    content: Box<dyn SeekableStream>,
    parallel: usize,
    partition_size: usize,
    client: &impl PartitionedUploadBehavior,
) -> azure_core::Result<()> {
    let partitions = PartitionedStream::new(content, partition_size)
        .map_ok(|vec_bytes| MultiBytesStream::new(vec_bytes.clone()));
    let ops = partitions.map(|res| {
        async || match res {
            Ok(stream) => {
                client
                    .transfer_partition(Body::SeekableStream(Box::new(stream)))
                    .await
            }
            Err(e) => Err(e),
        }
    });
    run_all_with_concurrency_limit(ops, parallel).await?;
    Ok(())
}

async fn run_all_with_concurrency_limit<TFut, TErr>(
    mut ops: impl Stream<Item = impl FnOnce() -> TFut> + Unpin,
    parallel: usize,
) -> Result<(), TErr>
where
    TFut: Future<Output = Result<(), TErr>>,
{
    let mut running_ops = Vec::with_capacity(parallel);

    // This loop fills running_ops before it lets a single op actually run
    // fine for in-memory source but not for IO streams
    // need else case where fetching next and executing current are either'd
    while let Some(op) = ops.next().await {
        running_ops.push(Box::pin(op()));
        if running_ops.len() >= parallel {
            let result;
            (result, _, running_ops) = futures::future::select_all(running_ops).await;
            result?;
        }
    }
    while !running_ops.is_empty() {
        let result;
        (result, _, running_ops) = futures::future::select_all(running_ops).await;
        result?;
    }
    Ok(())
}

fn div_round_up(left: usize, right: usize) -> usize {
    ((left as f64) / (right as f64)).ceil() as usize
}

impl crate::generated::BlockBlobClient {}
