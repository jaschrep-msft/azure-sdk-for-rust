use std::ops::Range;

use azure_core::http::Body;

use super::*;

pub(crate) trait PartitionedCopyBehavior {
    async fn get_ranges(&self, partition_size: u64) -> impl IntoIterator<Item = Range<u64>>;
    async fn transfer_range(&self, range: Range<u64>) -> AzureResult<()>;
    async fn initialize(&self) -> AzureResult<()>;
    async fn finalize(&self) -> AzureResult<()>;
}

pub(crate) async fn copy(
    parallel: usize,
    partition_size: u64,
    client: &impl PartitionedCopyBehavior,
) -> AzureResult<()> {
    client.initialize().await?;

    let ops = client
        .get_ranges(partition_size)
        .await
        .into_iter()
        .map(|range| Ok(|| client.transfer_range(range)));
    run_all_with_concurrency_limit(futures::stream::iter(ops), parallel).await?;

    client.finalize().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, cmp};

    use super::*;

    #[derive(Debug, Clone)]
    enum MockPartitionedCopyBehaviorInvocation {
        GetRanges(u64),
        TransferPartition(Range<u64>),
        Initialize(),
        Finalize(),
    }

    struct MockPartitionedCopyBehavior {
        pub invocations: RefCell<Vec<MockPartitionedCopyBehaviorInvocation>>,
        pub data_len: u64,
    }

    impl MockPartitionedCopyBehavior {
        pub fn new(data_len: u64) -> Self {
            Self {
                invocations: RefCell::new(vec![]),
                data_len,
            }
        }
    }

    impl PartitionedCopyBehavior for MockPartitionedCopyBehavior {
        async fn get_ranges(&self, partition_size: u64) -> impl IntoIterator<Item = Range<u64>> {
            self.invocations
                .borrow_mut()
                .push(MockPartitionedCopyBehaviorInvocation::GetRanges(
                    partition_size,
                ));
            let partitions = div_round_up(self.data_len as usize, partition_size as usize) as u64;
            (0..partitions)
                .map(|part| {
                    part * partition_size
                        ..cmp::min(part * partition_size + partition_size, self.data_len)
                })
                .collect::<Vec<_>>()
        }

        async fn transfer_range(&self, range: Range<u64>) -> AzureResult<()> {
            self.invocations.borrow_mut().push(
                MockPartitionedCopyBehaviorInvocation::TransferPartition(range),
            );
            Ok(())
        }

        async fn initialize(&self) -> AzureResult<()> {
            self.invocations
                .borrow_mut()
                .push(MockPartitionedCopyBehaviorInvocation::Initialize());
            Ok(())
        }

        async fn finalize(&self) -> AzureResult<()> {
            self.invocations
                .borrow_mut()
                .push(MockPartitionedCopyBehaviorInvocation::Finalize());
            Ok(())
        }
    }

    #[tokio::test]
    #[allow(clippy::single_range_in_vec_init)]
    async fn single_copy() -> AzureResult<()> {
        let data_len = 1024u64;
        let partition_size = data_len;
        let parallel: usize = 2;
        let expected_ranges = vec![0..data_len];
        let mock = MockPartitionedCopyBehavior::new(data_len);

        copy(parallel, partition_size, &mock).await?;
        assert_partitioned_copy_invocations(&mock, expected_ranges, partition_size);
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::single_range_in_vec_init)]
    async fn single_copy_under_partition() -> AzureResult<()> {
        let data_len = 1024u64;
        let partition_size = 2048u64;
        let parallel: usize = 2;
        let expected_ranges = vec![0..data_len];
        let mock = MockPartitionedCopyBehavior::new(data_len);

        copy(parallel, partition_size, &mock).await?;
        assert_partitioned_copy_invocations(&mock, expected_ranges, partition_size);
        Ok(())
    }

    #[tokio::test]
    async fn multipart_copy() -> AzureResult<()> {
        let data_len = 1024u64;
        let partition_size = 512u64;
        let parallel: usize = 2;
        let expected_ranges = vec![0..partition_size, partition_size..data_len];
        let mock = MockPartitionedCopyBehavior::new(data_len);

        copy(parallel, partition_size, &mock).await?;
        assert_partitioned_copy_invocations(&mock, expected_ranges, partition_size);
        Ok(())
    }

    #[tokio::test]
    async fn multipart_copy_under_partition() -> AzureResult<()> {
        let data_len = 1024u64;
        let partition_size = 1000u64;
        let parallel: usize = 2;
        let expected_ranges = vec![0..partition_size, partition_size..data_len];
        let mock = MockPartitionedCopyBehavior::new(data_len);

        copy(parallel, partition_size, &mock).await?;
        assert_partitioned_copy_invocations(&mock, expected_ranges, partition_size);
        Ok(())
    }

    fn assert_partitioned_copy_invocations(
        mock: &MockPartitionedCopyBehavior,
        expected_ranges: impl IntoIterator<Item = Range<u64>>,
        partition_size: u64,
    ) {
        let mut expected_ranges: Vec<_> = expected_ranges.into_iter().collect();
        expected_ranges.sort_by(|left_range, right_range| left_range.start.cmp(&right_range.start));

        let invocations = mock.invocations.borrow().clone();

        assert_eq!(invocations.len(), expected_ranges.len() + 3);
        assert!(matches!(
            &invocations[0],
            MockPartitionedCopyBehaviorInvocation::Initialize()
        ));
        assert!(matches!(
            &invocations[1],
            MockPartitionedCopyBehaviorInvocation::GetRanges(len) if *len == partition_size
        ));
        assert!(matches!(
            &invocations[invocations.len() - 1],
            MockPartitionedCopyBehaviorInvocation::Finalize()
        ));

        let mut sorted_transfer_partition_invocations: Vec<_> = invocations
            .iter()
            .filter_map(|invocation| match invocation {
                MockPartitionedCopyBehaviorInvocation::TransferPartition(range) => {
                    Some(range.clone())
                }
                _ => None,
            })
            .collect();
        sorted_transfer_partition_invocations
            .sort_by(|left_range, right_range| left_range.start.cmp(&right_range.start));

        assert_eq!(sorted_transfer_partition_invocations, expected_ranges);
    }
}
