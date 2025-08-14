use azure_core::{stream::SeekableStream, time::OffsetDateTime};

use crate::generated::models::EncryptionAlgorithmType;

pub trait AccessConditionOptionsExt {
    fn with_access_conditions(
        self,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_modified_since: Option<OffsetDateTime>,
        if_unmodified_since: Option<OffsetDateTime>,
    ) -> Self;

    fn with_no_overwrite(self) -> Self
    where
        Self: std::marker::Sized,
    {
        self.with_access_conditions(None, Some("*".to_string()), None, None)
    }
}

pub trait CustomerProvidedKeyOptionsExt {
    fn with_customer_provided_key_base64(
        self,
        encryption_key_base64: String,
        encryption_key_sha256_base64: String,
        algorithm_type: EncryptionAlgorithmType,
    ) -> Self;
}

pub trait TransferValidationOptionsExt {
    fn with_no_transfer_validation(self) -> Self;
    fn with_transactional_crc64(self, crc64: u64) -> Self;
}
