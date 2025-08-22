use async_channel::{self, Receiver, Sender};
use azure_core::http::{Body, RequestContent};
use futures::StreamExt;
use uuid::Uuid;

use crate::{
    generated::BlockBlobClient,
    models::{Block, BlockLookupList},
    partitioned_transfer::*,
};

impl BlockBlobClient {
    async fn managed_upload(&self, body: Body) -> azure_core::Result<()> {
        upload(body, 1, 1024, &BlockBlobClientUploadBehavior::new(self)).await?;
        Ok(())
    }
}

struct BlockInfo {
    offset: usize,
    block_id: Uuid,
}

struct BlockBlobClientUploadBehavior<'a> {
    client: &'a BlockBlobClient,
    blocks_sender: Sender<BlockInfo>,
    blocks_receiver: Receiver<BlockInfo>,
}

impl<'a> BlockBlobClientUploadBehavior<'a> {
    fn new(client: &'a BlockBlobClient) -> Self {
        let (blocks_sender, blocks_receiver) = async_channel::unbounded();
        BlockBlobClientUploadBehavior {
            client,
            blocks_sender,
            blocks_receiver,
        }
    }
}

impl PartitionedUploadBehavior for BlockBlobClientUploadBehavior<'_> {
    async fn transfer_oneshot(&self, content: Body) -> azure_core::Result<()> {
        let content_len = content.len().try_into().unwrap();
        self.client
            .upload(content.into(), content_len, None)
            .await?;
        Ok(())
    }

    async fn transfer_partition(&self, offset: usize, content: Body) -> azure_core::Result<()> {
        let block_id = Uuid::new_v4();
        let content_len = content.len().try_into().unwrap();
        self.blocks_sender
            .send(BlockInfo { offset, block_id })
            .await
            .unwrap(); // TODO unwrap
        self.client
            .stage_block(block_id.as_bytes(), content_len, content.into(), None)
            .await?;
        Ok(())
    }

    async fn initialize(&self, _content_len: usize) -> azure_core::Result<()> {
        Ok(())
    }

    async fn finalize(&self) -> azure_core::Result<()> {
        let mut blocks: Vec<_> = self.blocks_receiver.clone().collect().await; // TODO is this really the right way to do this?
        blocks.sort_by(|left, right| left.offset.cmp(&right.offset));
        let blocklist = BlockLookupList {
            committed: Some(
                blocks
                    .iter()
                    .map(|bi| bi.block_id.as_bytes().to_vec())
                    .collect(),
            ),
            ..Default::default()
        };
        self.client
            .commit_block_list(blocklist.try_into()?, None)
            .await?;

        Ok(())
    }
}
