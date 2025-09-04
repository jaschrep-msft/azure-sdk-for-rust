use std::ops::Range;

use azure_core::http::{RawResponse, Response};
use bytes::Bytes;
use futures::Stream;

use crate::{models::BlobClientDownloadOptions, partitioned_transfer::*, BlobClient};

impl BlobClient {
    async fn managed_download(
        &self,
    ) -> azure_core::Result<impl Stream<Item = azure_core::Result<Bytes>> + use<'_>> {
        download(1, 1024, &BlockBlobClientDownloadBehavior::new(self)).await
    }
}

struct BlockBlobClientDownloadBehavior<'a> {
    client: &'a BlobClient,
}

impl<'a> BlockBlobClientDownloadBehavior<'a> {
    fn new(client: &'a BlobClient) -> Self {
        Self { client }
    }
}

impl PartitionedDownloadBehavior for BlockBlobClientDownloadBehavior<'_> {
    async fn transfer_range(&self, range: Range<u64>) -> azure_core::Result<RawResponse> {
        Ok(self
            .client
            .download(Some(BlobClientDownloadOptions {
                range: Some(format!("bytes={}-{}", range.start, range.end - 1)),
                ..Default::default()
            }))
            .await?
            .into())
    }
}
