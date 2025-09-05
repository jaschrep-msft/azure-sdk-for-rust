use std::{collections::VecDeque, ops::Range, task::Poll};

use azure_core::{
    http::{request::options::ContentRange, response::ResponseBody, RawResponse, Response},
    Error,
};
use bytes::Bytes;
use futures::{
    future::{self, BoxFuture},
    ready, FutureExt,
};

use super::*;

pub(crate) trait PartitionedDownloadBehavior {
    fn transfer_range(
        &self,
        range: Range<u64>,
    ) -> impl Future<Output = AzureResult<RawResponse>> + Send;
}

pub(crate) async fn download<T: PartitionedDownloadBehavior>(
    parallel: usize,
    partition_size: usize,
    client: &'_ T,
) -> AzureResult<impl Stream<Item = AzureResult<Bytes>> + use<'_, T>> {
    let err = || {
        azure_core::Error::message(
            azure_core::error::ErrorKind::Other,
            "Failed to parse Content-Range header {}.",
        )
    };
    let partition_size = partition_size as u64;
    let initial_response = client.transfer_range(0..partition_size).await?;
    let content_range: ContentRange = initial_response
        .headers()
        .get_optional_as(&"content-range".into())?
        .ok_or_else(err)?;

    let total_ranges =
        ((content_range.total_length() as f64) / (partition_size as f64)).ceil() as u64;

    let mut ranges = (1u64..total_ranges)
        .map(move |i| (i * partition_size..i * partition_size + partition_size));

    let mut ops = VecDeque::from([TransferOp::Transferring(
        initial_response.into_body().collect().boxed(),
    )]);

    let stream = futures::stream::poll_fn(move |cx| {
        while ops.len() < parallel {
            match ranges.next() {
                Some(range) => ops.push_back(TransferOp::AwaitingResponse(
                    client.transfer_range(range).boxed(),
                )),
                None => break,
            }
        }

        for op in ops.iter_mut() {
            // rechecking the op if state changed handles edge case where the next step of the transfer
            // immediately returns completed. needed for mock synchronous implementations
            let mut check_op = true;
            while check_op {
                check_op = false;
                match op {
                    TransferOp::AwaitingResponse(fut) => {
                        if let Poll::Ready(res) = fut.as_mut().poll(cx) {
                            match res {
                                Ok(response) => {
                                    *op = TransferOp::Transferring(
                                        response.into_body().collect().boxed(),
                                    );
                                    check_op = true;
                                }
                                Err(e) => return Poll::Ready(Some(Err(e))),
                            }
                        }
                    }
                    TransferOp::Transferring(fut) => {
                        if let Poll::Ready(res) = fut.as_mut().poll(cx) {
                            match res {
                                Ok(bytes) => {
                                    *op = TransferOp::Ready(bytes);
                                    check_op = true;
                                }
                                Err(e) => return Poll::Ready(Some(Err(e))),
                            }
                        }
                    }
                    TransferOp::Ready(_bytes) => {}
                }
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

    Ok(stream)
}

enum TransferOp<FutResponse, FutBytes> {
    AwaitingResponse(FutResponse),
    Transferring(FutBytes),
    Ready(Bytes),
}

#[cfg(test)]
mod tests {
    use std::{
        cell::RefCell,
        cmp::{max, min},
    };

    use azure_core::{
        http::{headers::Headers, RawResponse, StatusCode},
        stream::BytesStream,
    };
    use futures::StreamExt;
    use rand::Rng;
    use tokio::{
        sync::Mutex,
        time::{sleep, Duration},
    };

    use super::*;

    #[derive(Debug)]
    enum MockPartitionedDownloadBehaviorInvocation {
        TransferRange(Range<u64>),
    }

    struct MockPartitionedDownloadBehavior {
        pub invocations: Mutex<Vec<MockPartitionedDownloadBehaviorInvocation>>,
        pub data: Bytes,
        pub delay_millis: Option<Range<u64>>,
    }

    impl MockPartitionedDownloadBehavior {
        pub fn new(data: impl Into<Bytes>, delay_millis: Option<Range<u64>>) -> Self {
            Self {
                invocations: Mutex::new(vec![]),
                data: data.into(),
                delay_millis,
            }
        }
    }

    impl PartitionedDownloadBehavior for MockPartitionedDownloadBehavior {
        async fn transfer_range(&self, range: Range<u64>) -> AzureResult<RawResponse> {
            {
                self.invocations.lock().await.push(
                    MockPartitionedDownloadBehaviorInvocation::TransferRange(range.clone()),
                );
            }

            if let Some(delay_millis_range) = self.delay_millis.clone() {
                let millis = rand::random_range(delay_millis_range);
                sleep(Duration::from_millis(millis)).await
            }

            let range = max(range.start, 0)..min(range.end, self.data.len() as u64);
            let mut headers = Headers::new();
            headers.insert(
                "content-range",
                ContentRange::new(range.start, range.end - 1, self.data.len() as u64).to_string(),
            );
            let range = range.start as usize..range.end as usize;
            let raw = RawResponse::new(
                StatusCode::PartialContent,
                headers,
                Box::pin(BytesStream::from(self.data.slice(range))),
            );
            Ok(raw)
        }
    }

    #[tokio::test]
    async fn download_single_range_oversized() -> AzureResult<()> {
        let data_size: usize = 123;
        let partition_size: usize = 1024;
        let parallel: usize = 2;

        let data = get_random_data(data_size);
        let mock = MockPartitionedDownloadBehavior::new(data.clone(), None);

        let downloaded_data = download(parallel, partition_size, &mock)
            .await?
            .buffer_all()
            .await?;

        assert_eq!(downloaded_data[..], data[..]);
        assert_eq!(mock.invocations.lock().await.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn download_single_range_exact() -> AzureResult<()> {
        let data_size: usize = 1024;
        let partition_size: usize = 1024;
        let parallel: usize = 2;

        let data = get_random_data(data_size);
        let mock = MockPartitionedDownloadBehavior::new(data.clone(), None);

        let downloaded_data = download(parallel, partition_size, &mock)
            .await?
            .buffer_all()
            .await?;

        assert_eq!(downloaded_data[..], data[..]);
        assert_eq!(mock.invocations.lock().await.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn download_multi_range_exact() -> AzureResult<()> {
        let segments = 8;
        let data_size: usize = 1024 * segments;
        let partition_size: usize = 1024;
        let parallel: usize = 2;

        let data = get_random_data(data_size);
        let mock = MockPartitionedDownloadBehavior::new(data.clone(), None);

        let downloaded_data = download(parallel, partition_size, &mock)
            .await?
            .buffer_all()
            .await?;

        assert_eq!(downloaded_data[..], data[..]);
        assert_eq!(mock.invocations.lock().await.len(), segments);

        Ok(())
    }

    #[tokio::test]
    async fn download_multi_range_partial() -> AzureResult<()> {
        let segments = 8;
        let data_size: usize = 1024 * (segments - 1) + 123;
        let partition_size: usize = 1024;
        let parallel: usize = 2;

        let data = get_random_data(data_size);
        let mock = MockPartitionedDownloadBehavior::new(data.clone(), None);

        let downloaded_data = download(parallel, partition_size, &mock)
            .await?
            .buffer_all()
            .await?;

        assert_eq!(downloaded_data[..], data[..]);
        assert_eq!(mock.invocations.lock().await.len(), segments);

        Ok(())
    }

    #[tokio::test]
    async fn download_ranges_sequential() -> AzureResult<()> {
        let segments: usize = 8;
        let partition_size: usize = 1024;
        let data_size: usize = partition_size * segments;
        let parallel: usize = 1;

        let data = get_random_data(data_size);
        let mock = MockPartitionedDownloadBehavior::new(data.clone(), None);

        let downloaded_data = download(parallel, partition_size, &mock)
            .await?
            .buffer_all()
            .await?;

        assert_eq!(downloaded_data[..], data[..]);
        assert_eq!(mock.invocations.lock().await.len(), segments);

        Ok(())
    }

    #[tokio::test]
    async fn download_ranges_parallel_maintain_order() -> AzureResult<()> {
        let segments: usize = 20;
        let partition_size: usize = 3;
        let data_size: usize = partition_size * segments;
        let parallel: usize = 16;

        let data = get_random_data(data_size);
        let mock = MockPartitionedDownloadBehavior::new(data.clone(), Some(1..5));

        let downloaded_data = download(parallel, partition_size, &mock)
            .await?
            .buffer_all()
            .await?;

        assert_eq!(downloaded_data[..], data[..]);
        assert_eq!(mock.invocations.lock().await.len(), segments);

        Ok(())
    }

    trait BytesTryStreamExt {
        async fn buffer_all(&mut self) -> AzureResult<Vec<u8>>;
    }
    impl<S: Stream<Item = Result<Bytes, Error>> + Unpin> BytesTryStreamExt for S {
        async fn buffer_all(&mut self) -> AzureResult<Vec<u8>> {
            let mut buffer = Vec::<u8>::new();
            while let Some(bytes) = self.try_next().await? {
                buffer.extend_from_slice(&bytes);
            }

            Ok(buffer)
        }
    }

    fn get_random_data(len: usize) -> Vec<u8> {
        let mut data: Vec<u8> = vec![0; len];
        rand::fill(&mut data[..]);
        data
    }
}
