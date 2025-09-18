use std::{cmp, future::Future};

use async_channel::{self, Receiver, Sender};
use azure_core::{
    http::{Body, RequestContent},
    Error,
};
use futures::StreamExt;
use uuid::Uuid;

type AzureResult<T> = azure_core::Result<T>;

use crate::{
    generated::{models::BlockBlobClientStageBlockFromUrlOptions, BlobClient, BlockBlobClient},
    models::{BlobClientGetPropertiesResultHeaders, Block, BlockLookupList},
    partitioned_transfer::{self, PartitionedCopyBehavior, PartitionedUploadBehavior},
};

// implement this on handwritten client for now
impl crate::BlockBlobClient {
    pub async fn managed_upload(
        &self,
        body: Body,
        parallel: usize,
        partition_size: usize,
    ) -> AzureResult<()> {
        self.client
            .managed_upload(body, parallel, partition_size)
            .await
    }

    pub async fn managed_copy_from_url(
        &self,
        source: &impl ManagedCopySource,
        parallel: usize,
        partition_size: u64,
    ) -> AzureResult<()> {
        self.client
            .managed_copy_from_url(source, parallel, partition_size)
            .await
    }
}

impl BlockBlobClient {
    pub async fn managed_upload(
        &self,
        body: Body,
        parallel: usize,
        partition_size: usize,
    ) -> AzureResult<()> {
        partitioned_transfer::upload(
            body,
            parallel,
            partition_size,
            &BlockBlobClientUploadBehavior::new(self),
        )
        .await
    }

    pub async fn managed_copy_from_url(
        &self,
        source: &impl ManagedCopySource,
        parallel: usize,
        partition_size: u64,
    ) -> AzureResult<()> {
        let info = source.copy_source_info().await?;
        partitioned_transfer::copy(
            parallel,
            partition_size,
            &BlockBlobClientCopyBehavior::new(self, info),
        )
        .await
    }
}

pub struct ManagedCopySourceInfo {
    pub auth: Option<String>,
    pub len: u64,
    pub url: String,
}

pub trait ManagedCopySource {
    fn copy_source_info(&self) -> impl Future<Output = AzureResult<ManagedCopySourceInfo>>;
}

struct BlockInfo {
    offset: u64,
    block_id: Uuid,
}

struct BlockBlobClientUploadBehavior<'a> {
    client: &'a BlockBlobClient,
    blocks_sender: Sender<BlockInfo>,
    blocks_receiver: Receiver<BlockInfo>,
}

struct BlockBlobClientCopyBehavior<'a> {
    client: &'a BlockBlobClient,
    blocks_sender: Sender<BlockInfo>,
    blocks_receiver: Receiver<BlockInfo>,
    source_info: ManagedCopySourceInfo,
}

impl<'a> BlockBlobClientUploadBehavior<'a> {
    fn new(client: &'a BlockBlobClient) -> Self {
        let (blocks_sender, blocks_receiver) = async_channel::unbounded();
        Self {
            client,
            blocks_sender,
            blocks_receiver,
        }
    }
}

impl<'a> BlockBlobClientCopyBehavior<'a> {
    fn new(client: &'a BlockBlobClient, source_info: ManagedCopySourceInfo) -> Self {
        let (blocks_sender, blocks_receiver) = async_channel::unbounded();
        Self {
            client,
            blocks_sender,
            blocks_receiver,
            source_info,
        }
    }
}

impl PartitionedUploadBehavior for BlockBlobClientUploadBehavior<'_> {
    async fn transfer_oneshot(&self, content: Body) -> AzureResult<()> {
        let content_len = content.len().try_into().unwrap();
        self.client
            .upload(content.into(), content_len, None)
            .await?;
        Ok(())
    }

    async fn transfer_partition(&self, offset: usize, content: Body) -> AzureResult<()> {
        let block_id = Uuid::new_v4();
        let content_len = content.len().try_into().unwrap();
        self.blocks_sender
            .send(BlockInfo {
                offset: offset as u64,
                block_id,
            })
            .await
            .unwrap(); // TODO unwrap
        self.client
            .stage_block(block_id.as_bytes(), content_len, content.into(), None)
            .await?;
        Ok(())
    }

    async fn initialize(&self, _content_len: usize) -> AzureResult<()> {
        Ok(())
    }

    async fn finalize(&self) -> AzureResult<()> {
        commit_block_list(&self.blocks_sender, &self.blocks_receiver, self.client).await
    }
}

impl PartitionedCopyBehavior for BlockBlobClientCopyBehavior<'_> {
    async fn get_ranges(
        &self,
        partition_size: u64,
    ) -> impl IntoIterator<Item = std::ops::Range<u64>> {
        let partition_count = self.source_info.len.div_ceil(partition_size);
        (0..partition_count).map(move |i| {
            i * partition_size..cmp::min(i * partition_size + partition_size, self.source_info.len)
        })
    }

    async fn transfer_range(&self, range: std::ops::Range<u64>) -> AzureResult<()> {
        let block_id = Uuid::new_v4();
        self.blocks_sender
            .send(BlockInfo {
                offset: range.start,
                block_id,
            })
            .await
            .unwrap(); // TODO unwrap
        self.client
            .stage_block_from_url(
                block_id.as_bytes(),
                0,
                self.source_info.url.clone(),
                Some(BlockBlobClientStageBlockFromUrlOptions {
                    copy_source_authorization: self.source_info.auth.clone(),
                    source_range: Some(format!("{}-{}", range.start, range.end - 1)),
                    ..Default::default()
                }),
            )
            .await?;
        Ok(())
    }

    async fn initialize(&self) -> AzureResult<()> {
        Ok(())
    }

    async fn finalize(&self) -> AzureResult<()> {
        commit_block_list(&self.blocks_sender, &self.blocks_receiver, self.client).await
    }
}

async fn commit_block_list(
    blocks_sender: &Sender<BlockInfo>,
    blocks_receiver: &Receiver<BlockInfo>,
    client: &BlockBlobClient,
) -> AzureResult<()> {
    blocks_sender.close();
    let mut blocks: Vec<_> = blocks_receiver.clone().collect().await; // TODO dodge clone
    blocks.sort_by(|left, right| left.offset.cmp(&right.offset));
    let blocklist = BlockLookupList {
        latest: Some(
            blocks
                .iter()
                .map(|bi| bi.block_id.as_bytes().to_vec())
                .collect(),
        ),
        ..Default::default()
    };
    client
        .commit_block_list(blocklist.try_into()?, None)
        .await?;

    Ok(())
}

impl ManagedCopySource for BlobClient {
    async fn copy_source_info(&self) -> AzureResult<ManagedCopySourceInfo> {
        let len = self
            .get_properties(None)
            .await?
            .content_length()?
            .ok_or_else(|| {
                Error::new(
                    azure_core::error::ErrorKind::DataConversion,
                    "No content-length found",
                )
            })?;

        Ok(ManagedCopySourceInfo {
            auth: None,
            len,
            url: self.endpoint().to_string(),
        })
    }
}

impl ManagedCopySource for crate::BlobClient {
    async fn copy_source_info(&self) -> AzureResult<ManagedCopySourceInfo> {
        let len = self
            .get_properties(None)
            .await?
            .content_length()?
            .ok_or_else(|| {
                Error::new(
                    azure_core::error::ErrorKind::DataConversion,
                    "No content-length found",
                )
            })?;

        Ok(ManagedCopySourceInfo {
            auth: None,
            len,
            url: self.endpoint().to_string(),
        })
    }
}
