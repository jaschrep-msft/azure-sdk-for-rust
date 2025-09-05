use std::ops::Range;

use azure_core::http::{RawResponse, Response};
use bytes::Bytes;
use futures::{Stream, StreamExt};

use crate::{
    models::BlobClientDownloadOptions,
    partitioned_transfer::{self, PartitionedDownloadBehavior},
    BlobClient,
};

impl BlobClient {
    async fn managed_download(
        &self,
    ) -> azure_core::Result<impl Stream<Item = azure_core::Result<Bytes>> + use<'_>> {
        partitioned_transfer::download(1, 1024, self).await
    }
}

impl PartitionedDownloadBehavior for BlobClient {
    async fn transfer_range(&self, range: Range<u64>) -> azure_core::Result<RawResponse> {
        Ok(self
            .download(Some(BlobClientDownloadOptions {
                range: Some(format!("bytes={}-{}", range.start, range.end - 1)),
                ..Default::default()
            }))
            .await?
            .into())
    }
}
