use azure_core::http::{
    request::options::{ContentRange, Range},
    Response,
};
use bytes::Bytes;

use super::*;

pub(crate) trait PartitionedDownloadBehavior {
    async fn transfer_range(&self, range: Range) -> AzureResult<Response<()>>;
}

pub(crate) async fn download(
    parallel: usize,
    partition_size: usize,
    client: &impl PartitionedDownloadBehavior,
) -> AzureResult<Box<dyn Stream<Item = AzureResult<Bytes>>>> {
    let err = || {
        azure_core::Error::message(
            azure_core::error::ErrorKind::Other,
            "Failed to parse Content-Range header {}.",
        )
    };
    let initial_response = client.transfer_range((0..partition_size).into()).await?;
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
    //let ops: Stream<AzureResult<Box<dyn Stream<Item = AzureResult<Bytes>>>> = stream::once(future::ready(Ok()))

    todo!()
}
