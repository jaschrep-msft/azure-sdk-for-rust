use std::{cmp, io, pin::Pin, task::Poll};

use azure_core::stream::SeekableStream;
use bytes::Bytes;
use futures::{AsyncRead, Stream};

#[derive(Clone, Debug)]
pub(crate) struct MultiBytesStream {
    content: Vec<Bytes>,
    index: usize,
}

impl MultiBytesStream {
    pub(crate) fn new(content: Vec<Bytes>) -> MultiBytesStream {
        MultiBytesStream { content, index: 0 }
    }
}

impl Stream for MultiBytesStream {
    type Item = azure_core::Result<Bytes>;

    fn poll_next(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut buf_index: usize = 0;
        for bytes in self.content.iter() {
            if buf_index + bytes.len() <= self.index {
                buf_index += bytes.len();
                continue;
            }
            let item = if buf_index == self.index {
                bytes.clone()
            } else {
                bytes.slice((self.index - buf_index)..bytes.len())
            };
            self.get_mut().index += item.len();
            return Poll::Ready(Some(Ok(item)));
        }
        return Poll::Ready(None);
    }
}

impl AsyncRead for MultiBytesStream {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let self_mut = self.get_mut();
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if self_mut.index >= self_mut.len() {
            return Poll::Ready(Ok(0));
        }

        let bytes_to_copy = cmp::min(self_mut.len() - self_mut.index, buf.len());

        let mut copied: usize = 0;
        let mut enumerated_bytes: usize = 0;
        for bytes in self_mut.content.iter() {
            if copied >= bytes_to_copy {
                break;
            }
            if bytes.is_empty() {
                continue;
            }
            if enumerated_bytes + bytes.len() <= self_mut.index {
                enumerated_bytes += bytes.len();
                continue;
            }
            let bytes_index = self_mut.index - enumerated_bytes;
            let copy_len = cmp::min(buf.len() - copied, bytes.len() - bytes_index);

            let copy_src_slice = &bytes[bytes_index..(bytes_index + copy_len)];
            buf[copied..(copied + copy_len)].copy_from_slice(copy_src_slice);
            self_mut.index += copy_len;
            copied += copy_len;
            enumerated_bytes += bytes.len();
        }

        Poll::Ready(Ok(copied))
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl SeekableStream for MultiBytesStream {
    async fn reset(&mut self) -> azure_core::Result<()> {
        self.index = 0;
        Ok(())
    }

    fn len(&self) -> usize {
        self.content.iter().map(|bytes| bytes.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{AsyncReadExt, StreamExt, TryStreamExt};

    #[tokio::test]
    async fn poll_next() -> azure_core::Result<()> {
        let src = [b"AAAA", b"BBBB", b"CCCC"];

        let mut multi_bytes_stream = MultiBytesStream {
            content: src.iter().map(|slc| Bytes::from(slc.to_vec())).collect(),
            index: 0,
        };

        for (i, arr) in src.iter().enumerate() {
            assert_eq!(multi_bytes_stream.index, i * 4);
            assert_eq!(multi_bytes_stream.try_next().await?.unwrap()[..], arr[..]);
        }
        assert_eq!(multi_bytes_stream.index, 12);

        Ok(())
    }

    async fn poll_next_end() -> azure_core::Result<()> {
        let src = [b"AAAA", b"BBBB", b"CCCC"];

        let mut multi_bytes_stream = MultiBytesStream {
            content: src.iter().map(|slc| Bytes::from(slc.to_vec())).collect(),
            index: 12,
        };

        for _ in 0..3 {
            assert_eq!(multi_bytes_stream.try_next().await?, None);
            assert_eq!(multi_bytes_stream.index, 12);
        }

        Ok(())
    }

    #[tokio::test]
    async fn poll_next_partial() -> azure_core::Result<()> {
        let src = [b"AAAA", b"BBBB", b"CCCC"];

        let mut multi_bytes_stream = MultiBytesStream {
            content: src.iter().map(|slc| Bytes::from(slc.to_vec())).collect(),
            index: 2,
        };

        assert_eq!(multi_bytes_stream.try_next().await?.unwrap()[..], b"AA"[..]);

        for (i, arr) in src.iter().enumerate().skip(1) {
            assert_eq!(multi_bytes_stream.index, i * 4);
            assert_eq!(multi_bytes_stream.try_next().await?.unwrap()[..], arr[..]);
        }
        assert_eq!(multi_bytes_stream.index, 12);

        Ok(())
    }

    #[tokio::test]
    async fn poll_read() -> azure_core::Result<()> {
        let src = [b"AAAA", b"BBBB", b"CCCC"];

        let mut multi_bytes_stream = MultiBytesStream {
            content: src.iter().map(|slc| Bytes::from(slc.to_vec())).collect(),
            index: 0,
        };

        let mut dst: [u8; 12] = Default::default();
        assert_eq!(multi_bytes_stream.read(&mut dst).await?, 12);
        assert_eq!(&dst, b"AAAABBBBCCCC");

        Ok(())
    }

    async fn poll_read_end() -> azure_core::Result<()> {
        let src = [b"AAAA", b"BBBB", b"CCCC"];

        let mut multi_bytes_stream = MultiBytesStream {
            content: src.iter().map(|slc| Bytes::from(slc.to_vec())).collect(),
            index: 12,
        };

        let mut dst: [u8; 12] = Default::default();
        for _ in 0..3 {
            assert_eq!(multi_bytes_stream.read(&mut dst).await?, 0);
            assert_eq!(multi_bytes_stream.index, 12);
        }

        Ok(())
    }

    #[tokio::test]
    async fn poll_read_partial() -> azure_core::Result<()> {
        let src = [b"AAAA", b"BBBB", b"CCCC"];

        let mut multi_bytes_stream = MultiBytesStream {
            content: src.iter().map(|slc| Bytes::from(slc.to_vec())).collect(),
            index: 2,
        };

        let mut dst: [u8; 7] = Default::default();
        assert_eq!(multi_bytes_stream.read(&mut dst).await?, 7);
        assert_eq!(&dst, b"AABBBBC");
        assert_eq!(multi_bytes_stream.index, 9);

        Ok(())
    }
}
