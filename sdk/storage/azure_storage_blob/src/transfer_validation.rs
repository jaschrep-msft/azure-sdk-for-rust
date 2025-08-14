use std::future::Future;

use azure_core::{http::Body, stream::SeekableStream, Bytes, Error};
use futures::{stream::*, AsyncReadExt};

pub const CRC64_NVME: crc::Algorithm<u64> = crc::Algorithm {
    width: 64,
    poly: 0xAD93D23594C93659,
    init: 0xFFFFFFFFFFFFFFFF,
    refin: true,
    refout: true,
    xorout: 0xFFFFFFFFFFFFFFFF,
    check: 0xae8b14860a799888,
    residue: 0xf310303b2b6f6e42,
};

pub trait StorageCrc64Ext {
    fn calculate_transactional_crc64(&self) -> u64;
}

impl<T: AsRef<[u8]>> StorageCrc64Ext for T {
    fn calculate_transactional_crc64(&self) -> u64 {
        let crc = crc::Crc::<u64>::new(&CRC64_NVME);
        crc.checksum(self.as_ref())
    }
}

// pub trait AsyncCrc64Ext {
//     fn calculate_transactional_crc64(
//         &mut self,
//     ) -> impl std::future::Future<Output = Result<u64, Error>> + Send;
// }

// impl AsyncCrc64Ext for &dyn SeekableStream {
//     async fn calculate_transactional_crc64(&mut self) -> Result<u64, Error> {
//         let crc = crc::Crc::<u64>::new(&CRC64_NVME);
//         let mut digest = crc.digest();
//         while let Some(bytes) = self.next().await {
//             digest.update(&bytes?);
//         }
//         Ok(digest.finalize())
//     }
// }
