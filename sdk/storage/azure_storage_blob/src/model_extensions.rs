use azure_core::time::OffsetDateTime;

use crate::options_helpers::{AccessConditionOptionsExt, CustomerProvidedKeyOptionsExt};

impl AccessConditionOptionsExt
    for crate::generated::models::AppendBlobClientAppendBlockOptions<'_>
{
    fn with_access_conditions(
        mut self,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_modified_since: Option<OffsetDateTime>,
        if_unmodified_since: Option<OffsetDateTime>,
    ) -> Self {
        self.if_match = if_match;
        self.if_none_match = if_none_match;
        self.if_modified_since = if_modified_since;
        self.if_unmodified_since = if_unmodified_since;
        self
    }
}

impl AccessConditionOptionsExt
    for crate::generated::models::AppendBlobClientAppendBlockFromUrlOptions<'_>
{
    fn with_access_conditions(
        mut self,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_modified_since: Option<OffsetDateTime>,
        if_unmodified_since: Option<OffsetDateTime>,
    ) -> Self {
        self.if_match = if_match;
        self.if_none_match = if_none_match;
        self.if_modified_since = if_modified_since;
        self.if_unmodified_since = if_unmodified_since;
        self
    }
}

impl AccessConditionOptionsExt for crate::generated::models::AppendBlobClientCreateOptions<'_> {
    fn with_access_conditions(
        mut self,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_modified_since: Option<OffsetDateTime>,
        if_unmodified_since: Option<OffsetDateTime>,
    ) -> Self {
        self.if_match = if_match;
        self.if_none_match = if_none_match;
        self.if_modified_since = if_modified_since;
        self.if_unmodified_since = if_unmodified_since;
        self
    }
}

impl AccessConditionOptionsExt for crate::generated::models::AppendBlobClientSealOptions<'_> {
    fn with_access_conditions(
        mut self,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_modified_since: Option<OffsetDateTime>,
        if_unmodified_since: Option<OffsetDateTime>,
    ) -> Self {
        self.if_match = if_match;
        self.if_none_match = if_none_match;
        self.if_modified_since = if_modified_since;
        self.if_unmodified_since = if_unmodified_since;
        self
    }
}

impl AccessConditionOptionsExt for crate::generated::models::BlobClientAcquireLeaseOptions<'_> {
    fn with_access_conditions(
        mut self,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_modified_since: Option<OffsetDateTime>,
        if_unmodified_since: Option<OffsetDateTime>,
    ) -> Self {
        self.if_match = if_match;
        self.if_none_match = if_none_match;
        self.if_modified_since = if_modified_since;
        self.if_unmodified_since = if_unmodified_since;
        self
    }
}

impl AccessConditionOptionsExt for crate::generated::models::BlobClientBreakLeaseOptions<'_> {
    fn with_access_conditions(
        mut self,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_modified_since: Option<OffsetDateTime>,
        if_unmodified_since: Option<OffsetDateTime>,
    ) -> Self {
        self.if_match = if_match;
        self.if_none_match = if_none_match;
        self.if_modified_since = if_modified_since;
        self.if_unmodified_since = if_unmodified_since;
        self
    }
}

impl AccessConditionOptionsExt for crate::generated::models::BlobClientChangeLeaseOptions<'_> {
    fn with_access_conditions(
        mut self,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_modified_since: Option<OffsetDateTime>,
        if_unmodified_since: Option<OffsetDateTime>,
    ) -> Self {
        self.if_match = if_match;
        self.if_none_match = if_none_match;
        self.if_modified_since = if_modified_since;
        self.if_unmodified_since = if_unmodified_since;
        self
    }
}

impl AccessConditionOptionsExt for crate::generated::models::BlobClientCopyFromUrlOptions<'_> {
    fn with_access_conditions(
        mut self,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_modified_since: Option<OffsetDateTime>,
        if_unmodified_since: Option<OffsetDateTime>,
    ) -> Self {
        self.if_match = if_match;
        self.if_none_match = if_none_match;
        self.if_modified_since = if_modified_since;
        self.if_unmodified_since = if_unmodified_since;
        self
    }
}

impl AccessConditionOptionsExt for crate::generated::models::BlobClientCreateSnapshotOptions<'_> {
    fn with_access_conditions(
        mut self,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_modified_since: Option<OffsetDateTime>,
        if_unmodified_since: Option<OffsetDateTime>,
    ) -> Self {
        self.if_match = if_match;
        self.if_none_match = if_none_match;
        self.if_modified_since = if_modified_since;
        self.if_unmodified_since = if_unmodified_since;
        self
    }
}

impl AccessConditionOptionsExt for crate::generated::models::BlobClientDeleteOptions<'_> {
    fn with_access_conditions(
        mut self,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_modified_since: Option<OffsetDateTime>,
        if_unmodified_since: Option<OffsetDateTime>,
    ) -> Self {
        self.if_match = if_match;
        self.if_none_match = if_none_match;
        self.if_modified_since = if_modified_since;
        self.if_unmodified_since = if_unmodified_since;
        self
    }
}

impl AccessConditionOptionsExt for crate::generated::models::BlobClientDownloadOptions<'_> {
    fn with_access_conditions(
        mut self,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_modified_since: Option<OffsetDateTime>,
        if_unmodified_since: Option<OffsetDateTime>,
    ) -> Self {
        self.if_match = if_match;
        self.if_none_match = if_none_match;
        self.if_modified_since = if_modified_since;
        self.if_unmodified_since = if_unmodified_since;
        self
    }
}

impl AccessConditionOptionsExt for crate::generated::models::BlobClientGetPropertiesOptions<'_> {
    fn with_access_conditions(
        mut self,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_modified_since: Option<OffsetDateTime>,
        if_unmodified_since: Option<OffsetDateTime>,
    ) -> Self {
        self.if_match = if_match;
        self.if_none_match = if_none_match;
        self.if_modified_since = if_modified_since;
        self.if_unmodified_since = if_unmodified_since;
        self
    }
}

impl AccessConditionOptionsExt for crate::generated::models::BlobClientReleaseLeaseOptions<'_> {
    fn with_access_conditions(
        mut self,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_modified_since: Option<OffsetDateTime>,
        if_unmodified_since: Option<OffsetDateTime>,
    ) -> Self {
        self.if_match = if_match;
        self.if_none_match = if_none_match;
        self.if_modified_since = if_modified_since;
        self.if_unmodified_since = if_unmodified_since;
        self
    }
}

impl AccessConditionOptionsExt for crate::generated::models::BlobClientRenewLeaseOptions<'_> {
    fn with_access_conditions(
        mut self,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_modified_since: Option<OffsetDateTime>,
        if_unmodified_since: Option<OffsetDateTime>,
    ) -> Self {
        self.if_match = if_match;
        self.if_none_match = if_none_match;
        self.if_modified_since = if_modified_since;
        self.if_unmodified_since = if_unmodified_since;
        self
    }
}

impl AccessConditionOptionsExt for crate::generated::models::BlobClientSetMetadataOptions<'_> {
    fn with_access_conditions(
        mut self,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_modified_since: Option<OffsetDateTime>,
        if_unmodified_since: Option<OffsetDateTime>,
    ) -> Self {
        self.if_match = if_match;
        self.if_none_match = if_none_match;
        self.if_modified_since = if_modified_since;
        self.if_unmodified_since = if_unmodified_since;
        self
    }
}

impl AccessConditionOptionsExt for crate::generated::models::BlobClientSetPropertiesOptions<'_> {
    fn with_access_conditions(
        mut self,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_modified_since: Option<OffsetDateTime>,
        if_unmodified_since: Option<OffsetDateTime>,
    ) -> Self {
        self.if_match = if_match;
        self.if_none_match = if_none_match;
        self.if_modified_since = if_modified_since;
        self.if_unmodified_since = if_unmodified_since;
        self
    }
}

impl AccessConditionOptionsExt for crate::generated::models::BlobClientStartCopyFromUrlOptions<'_> {
    fn with_access_conditions(
        mut self,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_modified_since: Option<OffsetDateTime>,
        if_unmodified_since: Option<OffsetDateTime>,
    ) -> Self {
        self.if_match = if_match;
        self.if_none_match = if_none_match;
        self.if_modified_since = if_modified_since;
        self.if_unmodified_since = if_unmodified_since;
        self
    }
}

impl AccessConditionOptionsExt
    for crate::generated::models::BlockBlobClientCommitBlockListOptions<'_>
{
    fn with_access_conditions(
        mut self,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_modified_since: Option<OffsetDateTime>,
        if_unmodified_since: Option<OffsetDateTime>,
    ) -> Self {
        self.if_match = if_match;
        self.if_none_match = if_none_match;
        self.if_modified_since = if_modified_since;
        self.if_unmodified_since = if_unmodified_since;
        self
    }
}

impl AccessConditionOptionsExt
    for crate::generated::models::BlockBlobClientPutBlobFromUrlOptions<'_>
{
    fn with_access_conditions(
        mut self,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_modified_since: Option<OffsetDateTime>,
        if_unmodified_since: Option<OffsetDateTime>,
    ) -> Self {
        self.if_match = if_match;
        self.if_none_match = if_none_match;
        self.if_modified_since = if_modified_since;
        self.if_unmodified_since = if_unmodified_since;
        self
    }
}

impl AccessConditionOptionsExt for crate::generated::models::BlockBlobClientQueryOptions<'_> {
    fn with_access_conditions(
        mut self,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_modified_since: Option<OffsetDateTime>,
        if_unmodified_since: Option<OffsetDateTime>,
    ) -> Self {
        self.if_match = if_match;
        self.if_none_match = if_none_match;
        self.if_modified_since = if_modified_since;
        self.if_unmodified_since = if_unmodified_since;
        self
    }
}

impl AccessConditionOptionsExt for crate::generated::models::BlockBlobClientUploadOptions<'_> {
    fn with_access_conditions(
        mut self,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_modified_since: Option<OffsetDateTime>,
        if_unmodified_since: Option<OffsetDateTime>,
    ) -> Self {
        self.if_match = if_match;
        self.if_none_match = if_none_match;
        self.if_modified_since = if_modified_since;
        self.if_unmodified_since = if_unmodified_since;
        self
    }
}

impl AccessConditionOptionsExt for crate::generated::models::PageBlobClientClearPagesOptions<'_> {
    fn with_access_conditions(
        mut self,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_modified_since: Option<OffsetDateTime>,
        if_unmodified_since: Option<OffsetDateTime>,
    ) -> Self {
        self.if_match = if_match;
        self.if_none_match = if_none_match;
        self.if_modified_since = if_modified_since;
        self.if_unmodified_since = if_unmodified_since;
        self
    }
}

impl AccessConditionOptionsExt
    for crate::generated::models::PageBlobClientCopyIncrementalOptions<'_>
{
    fn with_access_conditions(
        mut self,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_modified_since: Option<OffsetDateTime>,
        if_unmodified_since: Option<OffsetDateTime>,
    ) -> Self {
        self.if_match = if_match;
        self.if_none_match = if_none_match;
        self.if_modified_since = if_modified_since;
        self.if_unmodified_since = if_unmodified_since;
        self
    }
}

impl AccessConditionOptionsExt for crate::generated::models::PageBlobClientCreateOptions<'_> {
    fn with_access_conditions(
        mut self,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_modified_since: Option<OffsetDateTime>,
        if_unmodified_since: Option<OffsetDateTime>,
    ) -> Self {
        self.if_match = if_match;
        self.if_none_match = if_none_match;
        self.if_modified_since = if_modified_since;
        self.if_unmodified_since = if_unmodified_since;
        self
    }
}

impl AccessConditionOptionsExt
    for crate::generated::models::PageBlobClientGetPageRangesOptions<'_>
{
    fn with_access_conditions(
        mut self,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_modified_since: Option<OffsetDateTime>,
        if_unmodified_since: Option<OffsetDateTime>,
    ) -> Self {
        self.if_match = if_match;
        self.if_none_match = if_none_match;
        self.if_modified_since = if_modified_since;
        self.if_unmodified_since = if_unmodified_since;
        self
    }
}

impl AccessConditionOptionsExt
    for crate::generated::models::PageBlobClientGetPageRangesDiffOptions<'_>
{
    fn with_access_conditions(
        mut self,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_modified_since: Option<OffsetDateTime>,
        if_unmodified_since: Option<OffsetDateTime>,
    ) -> Self {
        self.if_match = if_match;
        self.if_none_match = if_none_match;
        self.if_modified_since = if_modified_since;
        self.if_unmodified_since = if_unmodified_since;
        self
    }
}

impl AccessConditionOptionsExt for crate::generated::models::PageBlobClientResizeOptions<'_> {
    fn with_access_conditions(
        mut self,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_modified_since: Option<OffsetDateTime>,
        if_unmodified_since: Option<OffsetDateTime>,
    ) -> Self {
        self.if_match = if_match;
        self.if_none_match = if_none_match;
        self.if_modified_since = if_modified_since;
        self.if_unmodified_since = if_unmodified_since;
        self
    }
}

impl AccessConditionOptionsExt
    for crate::generated::models::PageBlobClientUpdateSequenceNumberOptions<'_>
{
    fn with_access_conditions(
        mut self,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_modified_since: Option<OffsetDateTime>,
        if_unmodified_since: Option<OffsetDateTime>,
    ) -> Self {
        self.if_match = if_match;
        self.if_none_match = if_none_match;
        self.if_modified_since = if_modified_since;
        self.if_unmodified_since = if_unmodified_since;
        self
    }
}

impl AccessConditionOptionsExt for crate::generated::models::PageBlobClientUploadPagesOptions<'_> {
    fn with_access_conditions(
        mut self,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_modified_since: Option<OffsetDateTime>,
        if_unmodified_since: Option<OffsetDateTime>,
    ) -> Self {
        self.if_match = if_match;
        self.if_none_match = if_none_match;
        self.if_modified_since = if_modified_since;
        self.if_unmodified_since = if_unmodified_since;
        self
    }
}

impl AccessConditionOptionsExt
    for crate::generated::models::PageBlobClientUploadPagesFromUrlOptions<'_>
{
    fn with_access_conditions(
        mut self,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_modified_since: Option<OffsetDateTime>,
        if_unmodified_since: Option<OffsetDateTime>,
    ) -> Self {
        self.if_match = if_match;
        self.if_none_match = if_none_match;
        self.if_modified_since = if_modified_since;
        self.if_unmodified_since = if_unmodified_since;
        self
    }
}

impl CustomerProvidedKeyOptionsExt
    for crate::generated::models::AppendBlobClientAppendBlockOptions<'_>
{
    fn with_customer_provided_key_base64(
        mut self,
        encryption_key_base64: String,
        encryption_key_sha256_base64: String,
        algorithm_type: crate::generated::models::EncryptionAlgorithmType,
    ) -> Self {
        self.encryption_key = Some(encryption_key_base64);
        self.encryption_key_sha256 = Some(encryption_key_sha256_base64);
        self.encryption_algorithm = Some(algorithm_type);
        self
    }
}

impl CustomerProvidedKeyOptionsExt
    for crate::generated::models::AppendBlobClientAppendBlockFromUrlOptions<'_>
{
    fn with_customer_provided_key_base64(
        mut self,
        encryption_key_base64: String,
        encryption_key_sha256_base64: String,
        algorithm_type: crate::generated::models::EncryptionAlgorithmType,
    ) -> Self {
        self.encryption_key = Some(encryption_key_base64);
        self.encryption_key_sha256 = Some(encryption_key_sha256_base64);
        self.encryption_algorithm = Some(algorithm_type);
        self
    }
}

impl CustomerProvidedKeyOptionsExt for crate::generated::models::AppendBlobClientCreateOptions<'_> {
    fn with_customer_provided_key_base64(
        mut self,
        encryption_key_base64: String,
        encryption_key_sha256_base64: String,
        algorithm_type: crate::generated::models::EncryptionAlgorithmType,
    ) -> Self {
        self.encryption_key = Some(encryption_key_base64);
        self.encryption_key_sha256 = Some(encryption_key_sha256_base64);
        self.encryption_algorithm = Some(algorithm_type);
        self
    }
}

impl CustomerProvidedKeyOptionsExt
    for crate::generated::models::BlobClientCreateSnapshotOptions<'_>
{
    fn with_customer_provided_key_base64(
        mut self,
        encryption_key_base64: String,
        encryption_key_sha256_base64: String,
        algorithm_type: crate::generated::models::EncryptionAlgorithmType,
    ) -> Self {
        self.encryption_key = Some(encryption_key_base64);
        self.encryption_key_sha256 = Some(encryption_key_sha256_base64);
        self.encryption_algorithm = Some(algorithm_type);
        self
    }
}

impl CustomerProvidedKeyOptionsExt for crate::generated::models::BlobClientDownloadOptions<'_> {
    fn with_customer_provided_key_base64(
        mut self,
        encryption_key_base64: String,
        encryption_key_sha256_base64: String,
        algorithm_type: crate::generated::models::EncryptionAlgorithmType,
    ) -> Self {
        self.encryption_key = Some(encryption_key_base64);
        self.encryption_key_sha256 = Some(encryption_key_sha256_base64);
        self.encryption_algorithm = Some(algorithm_type);
        self
    }
}

impl CustomerProvidedKeyOptionsExt
    for crate::generated::models::BlobClientGetPropertiesOptions<'_>
{
    fn with_customer_provided_key_base64(
        mut self,
        encryption_key_base64: String,
        encryption_key_sha256_base64: String,
        algorithm_type: crate::generated::models::EncryptionAlgorithmType,
    ) -> Self {
        self.encryption_key = Some(encryption_key_base64);
        self.encryption_key_sha256 = Some(encryption_key_sha256_base64);
        self.encryption_algorithm = Some(algorithm_type);
        self
    }
}

impl CustomerProvidedKeyOptionsExt for crate::generated::models::BlobClientSetMetadataOptions<'_> {
    fn with_customer_provided_key_base64(
        mut self,
        encryption_key_base64: String,
        encryption_key_sha256_base64: String,
        algorithm_type: crate::generated::models::EncryptionAlgorithmType,
    ) -> Self {
        self.encryption_key = Some(encryption_key_base64);
        self.encryption_key_sha256 = Some(encryption_key_sha256_base64);
        self.encryption_algorithm = Some(algorithm_type);
        self
    }
}

impl CustomerProvidedKeyOptionsExt
    for crate::generated::models::BlockBlobClientCommitBlockListOptions<'_>
{
    fn with_customer_provided_key_base64(
        mut self,
        encryption_key_base64: String,
        encryption_key_sha256_base64: String,
        algorithm_type: crate::generated::models::EncryptionAlgorithmType,
    ) -> Self {
        self.encryption_key = Some(encryption_key_base64);
        self.encryption_key_sha256 = Some(encryption_key_sha256_base64);
        self.encryption_algorithm = Some(algorithm_type);
        self
    }
}

impl CustomerProvidedKeyOptionsExt
    for crate::generated::models::BlockBlobClientPutBlobFromUrlOptions<'_>
{
    fn with_customer_provided_key_base64(
        mut self,
        encryption_key_base64: String,
        encryption_key_sha256_base64: String,
        algorithm_type: crate::generated::models::EncryptionAlgorithmType,
    ) -> Self {
        self.encryption_key = Some(encryption_key_base64);
        self.encryption_key_sha256 = Some(encryption_key_sha256_base64);
        self.encryption_algorithm = Some(algorithm_type);
        self
    }
}

impl CustomerProvidedKeyOptionsExt for crate::generated::models::BlockBlobClientQueryOptions<'_> {
    fn with_customer_provided_key_base64(
        mut self,
        encryption_key_base64: String,
        encryption_key_sha256_base64: String,
        algorithm_type: crate::generated::models::EncryptionAlgorithmType,
    ) -> Self {
        self.encryption_key = Some(encryption_key_base64);
        self.encryption_key_sha256 = Some(encryption_key_sha256_base64);
        self.encryption_algorithm = Some(algorithm_type);
        self
    }
}

impl CustomerProvidedKeyOptionsExt
    for crate::generated::models::BlockBlobClientStageBlockOptions<'_>
{
    fn with_customer_provided_key_base64(
        mut self,
        encryption_key_base64: String,
        encryption_key_sha256_base64: String,
        algorithm_type: crate::generated::models::EncryptionAlgorithmType,
    ) -> Self {
        self.encryption_key = Some(encryption_key_base64);
        self.encryption_key_sha256 = Some(encryption_key_sha256_base64);
        self.encryption_algorithm = Some(algorithm_type);
        self
    }
}

impl CustomerProvidedKeyOptionsExt
    for crate::generated::models::BlockBlobClientStageBlockFromUrlOptions<'_>
{
    fn with_customer_provided_key_base64(
        mut self,
        encryption_key_base64: String,
        encryption_key_sha256_base64: String,
        algorithm_type: crate::generated::models::EncryptionAlgorithmType,
    ) -> Self {
        self.encryption_key = Some(encryption_key_base64);
        self.encryption_key_sha256 = Some(encryption_key_sha256_base64);
        self.encryption_algorithm = Some(algorithm_type);
        self
    }
}

impl CustomerProvidedKeyOptionsExt for crate::generated::models::BlockBlobClientUploadOptions<'_> {
    fn with_customer_provided_key_base64(
        mut self,
        encryption_key_base64: String,
        encryption_key_sha256_base64: String,
        algorithm_type: crate::generated::models::EncryptionAlgorithmType,
    ) -> Self {
        self.encryption_key = Some(encryption_key_base64);
        self.encryption_key_sha256 = Some(encryption_key_sha256_base64);
        self.encryption_algorithm = Some(algorithm_type);
        self
    }
}

impl CustomerProvidedKeyOptionsExt
    for crate::generated::models::PageBlobClientClearPagesOptions<'_>
{
    fn with_customer_provided_key_base64(
        mut self,
        encryption_key_base64: String,
        encryption_key_sha256_base64: String,
        algorithm_type: crate::generated::models::EncryptionAlgorithmType,
    ) -> Self {
        self.encryption_key = Some(encryption_key_base64);
        self.encryption_key_sha256 = Some(encryption_key_sha256_base64);
        self.encryption_algorithm = Some(algorithm_type);
        self
    }
}

impl CustomerProvidedKeyOptionsExt for crate::generated::models::PageBlobClientCreateOptions<'_> {
    fn with_customer_provided_key_base64(
        mut self,
        encryption_key_base64: String,
        encryption_key_sha256_base64: String,
        algorithm_type: crate::generated::models::EncryptionAlgorithmType,
    ) -> Self {
        self.encryption_key = Some(encryption_key_base64);
        self.encryption_key_sha256 = Some(encryption_key_sha256_base64);
        self.encryption_algorithm = Some(algorithm_type);
        self
    }
}

impl CustomerProvidedKeyOptionsExt for crate::generated::models::PageBlobClientResizeOptions<'_> {
    fn with_customer_provided_key_base64(
        mut self,
        encryption_key_base64: String,
        encryption_key_sha256_base64: String,
        algorithm_type: crate::generated::models::EncryptionAlgorithmType,
    ) -> Self {
        self.encryption_key = Some(encryption_key_base64);
        self.encryption_key_sha256 = Some(encryption_key_sha256_base64);
        self.encryption_algorithm = Some(algorithm_type);
        self
    }
}

impl CustomerProvidedKeyOptionsExt
    for crate::generated::models::PageBlobClientUploadPagesOptions<'_>
{
    fn with_customer_provided_key_base64(
        mut self,
        encryption_key_base64: String,
        encryption_key_sha256_base64: String,
        algorithm_type: crate::generated::models::EncryptionAlgorithmType,
    ) -> Self {
        self.encryption_key = Some(encryption_key_base64);
        self.encryption_key_sha256 = Some(encryption_key_sha256_base64);
        self.encryption_algorithm = Some(algorithm_type);
        self
    }
}

impl CustomerProvidedKeyOptionsExt
    for crate::generated::models::PageBlobClientUploadPagesFromUrlOptions<'_>
{
    fn with_customer_provided_key_base64(
        mut self,
        encryption_key_base64: String,
        encryption_key_sha256_base64: String,
        algorithm_type: crate::generated::models::EncryptionAlgorithmType,
    ) -> Self {
        self.encryption_key = Some(encryption_key_base64);
        self.encryption_key_sha256 = Some(encryption_key_sha256_base64);
        self.encryption_algorithm = Some(algorithm_type);
        self
    }
}
