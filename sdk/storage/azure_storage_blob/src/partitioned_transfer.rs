use std::future::{self, Future};

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
    async fn transfer_partition(&self, offset: usize, content: Body) -> azure_core::Result<()>;
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
) -> azure_core::Result<()> {
    let partitions = PartitionedStream::new(content, partition_size)
        .map_ok(|vec_bytes| MultiBytesStream::new(vec_bytes.clone()))
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

async fn run_all_with_concurrency_limit<TFut, TErr>(
    mut ops: impl Stream<Item = Result<impl FnOnce() -> TFut, TErr>> + Unpin,
    parallel: usize,
) -> Result<(), TErr>
where
    TFut: Future<Output = Result<(), TErr>>,
{
    let mut running_ops = Vec::with_capacity(parallel);

    // This loop fills running_ops before it lets a single op actually run
    // fine for in-memory source but not for IO streams
    // need else case where fetching next and executing current are either'd
    while let Some(op) = ops.try_next().await? {
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

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, mem::discriminant, ops::Deref, sync::mpsc};

    use azure_core::Error;
    use futures::{AsyncRead, FutureExt};

    use super::*;

    #[derive(Debug, Clone, Copy)]
    enum BodyType {
        Bytes,
        SeekableStream,
    }

    #[derive(Debug)]
    enum MockPartitionedUploadBehaviorInvocation {
        Initialize(usize),
        TransferOneshot(Vec<u8>, BodyType),
        TransferPartition(usize, Vec<u8>, BodyType),
        Finalize(),
    }

    struct MockPartitionedUploadBehavior {
        pub invocations: RefCell<Vec<MockPartitionedUploadBehaviorInvocation>>,
    }

    impl MockPartitionedUploadBehavior {
        pub fn new() -> MockPartitionedUploadBehavior {
            MockPartitionedUploadBehavior {
                invocations: RefCell::new(vec![]),
            }
        }
    }

    impl PartitionedUploadBehavior for MockPartitionedUploadBehavior {
        async fn transfer_oneshot(&self, mut content: Body) -> azure_core::Result<()> {
            let body_type = match content {
                Body::Bytes(_) => BodyType::Bytes,
                Body::SeekableStream(_) => BodyType::SeekableStream,
            };
            let bytes = get_bytes(&mut content).await?;
            self.invocations.borrow_mut().push(
                MockPartitionedUploadBehaviorInvocation::TransferOneshot(bytes, body_type),
            );
            Ok(())
        }

        async fn transfer_partition(
            &self,
            offset: usize,
            mut content: Body,
        ) -> azure_core::Result<()> {
            let body_type = match content {
                Body::Bytes(_) => BodyType::Bytes,
                Body::SeekableStream(_) => BodyType::SeekableStream,
            };
            let bytes = get_bytes(&mut content).await?;
            self.invocations.borrow_mut().push(
                MockPartitionedUploadBehaviorInvocation::TransferPartition(
                    offset, bytes, body_type,
                ),
            );
            Ok(())
        }

        async fn initialize(&self, content_len: usize) -> azure_core::Result<()> {
            self.invocations.borrow_mut().push(
                MockPartitionedUploadBehaviorInvocation::Initialize(content_len),
            );
            Ok(())
        }

        async fn finalize(&self) -> azure_core::Result<()> {
            self.invocations
                .borrow_mut()
                .push(MockPartitionedUploadBehaviorInvocation::Finalize());
            Ok(())
        }
    }

    #[tokio::test]
    async fn one_shot_when_within_partition_size() -> azure_core::Result<()> {
        let data_size: usize = 1024;
        let partition_size: usize = data_size;
        let concurrency: usize = 2;

        let mock = MockPartitionedUploadBehavior::new();
        let src_data = get_random_data(data_size);

        upload(
            Body::Bytes(Bytes::from(src_data.clone())),
            concurrency,
            partition_size,
            &mock,
        )
        .await?;

        assert_upload_oneshot_invocations(&mock, &src_data[..], BodyType::Bytes);

        Ok(())
    }

    #[tokio::test]
    async fn partition_when_over_partition_size() -> azure_core::Result<()> {
        let data_size: usize = 1024;
        let partition_size: usize = 50;
        let concurrency: usize = 1;

        let mock = MockPartitionedUploadBehavior::new();
        let src_data = get_random_data(data_size);

        upload(
            Body::Bytes(Bytes::from(src_data.clone())),
            concurrency,
            partition_size,
            &mock,
        )
        .await?;

        assert_upload_partitioned_invocations(
            &mock,
            &src_data[..],
            partition_size,
            BodyType::Bytes,
        );

        Ok(())
    }

    //////////////////

    async fn try_read_to_end(content: &mut Box<dyn SeekableStream>) -> azure_core::Result<Vec<u8>> {
        let mut dst = vec![0u8; content.len()];
        let mut i = 0;
        loop {
            let read = content.read(&mut dst[i..]).await?;
            if read == 0 {
                break;
            }
            i += read;
        }

        Ok(dst)
    }
    // }

    async fn get_bytes(content: &mut Body) -> azure_core::Result<Vec<u8>> {
        match content {
            Body::Bytes(bytes) => Ok(bytes.to_vec()),
            Body::SeekableStream(seekable_stream) => try_read_to_end(seekable_stream).await,
        }
    }

    fn assert_upload_invocations(
        mock: &MockPartitionedUploadBehavior,
        original_data: &[u8],
        partition_size: usize,
        expected_body_type: BodyType,
    ) {
        if original_data.len() <= partition_size {
            assert_upload_oneshot_invocations(mock, original_data, expected_body_type);
        } else {
            assert_upload_partitioned_invocations(
                mock,
                original_data,
                partition_size,
                expected_body_type,
            );
        }
    }

    fn assert_upload_oneshot_invocations(
        mock: &MockPartitionedUploadBehavior,
        original_data: &[u8],
        expected_body_type: BodyType,
    ) {
        let invocations = mock.invocations.borrow();
        assert_eq!(invocations.len(), 1);
        assert!(matches!(
            &invocations[0],
            MockPartitionedUploadBehaviorInvocation::TransferOneshot(data, body_type)
                if data[..] == *original_data && discriminant(body_type) == discriminant(&expected_body_type)
        ));
    }

    fn assert_upload_partitioned_invocations(
        mock: &MockPartitionedUploadBehavior,
        original_data: &[u8],
        partition_size: usize,
        expected_body_type: BodyType,
    ) {
        let expected_partitions = div_round_up(original_data.len(), partition_size);
        let invocations = mock.invocations.borrow();

        assert_eq!(invocations.len(), expected_partitions + 2);
        assert!(matches!(
            &invocations[0],
            MockPartitionedUploadBehaviorInvocation::Initialize(size) if *size == original_data.len()
        ));
        assert!(matches!(
            &invocations[invocations.len() - 1],
            MockPartitionedUploadBehaviorInvocation::Finalize()
        ));

        let mut sorted_transfer_partition_invocations: Vec<_> = invocations
            .iter()
            .filter_map(|invocation| match invocation {
                MockPartitionedUploadBehaviorInvocation::TransferPartition(
                    offset,
                    body,
                    body_type,
                ) => Some((*offset, body.clone(), *body_type)),
                _ => None,
            })
            .collect();
        sorted_transfer_partition_invocations
            .sort_by(|(left_offset, _, _), (right_offset, _, _)| left_offset.cmp(right_offset));

        assert_eq!(
            sorted_transfer_partition_invocations.len(),
            mock.invocations.borrow().len() - 2
        );

        for (i, (offset, body, body_type)) in
            sorted_transfer_partition_invocations.iter().enumerate()
        {
            assert_eq!(*offset, i * partition_size);
            assert_eq!(body[..], original_data[*offset..*offset + body.len()]);
            assert_eq!(discriminant(body_type), discriminant(&expected_body_type));
        }
    }

    fn get_random_data(len: usize) -> Vec<u8> {
        let mut data: Vec<u8> = vec![0; len];
        rand::fill(&mut data[..]);
        data
    }
}
