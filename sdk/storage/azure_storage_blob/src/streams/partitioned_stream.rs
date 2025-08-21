use std::{
    mem,
    ops::Deref,
    pin::{pin, Pin},
    task::Poll,
};

use azure_core::stream::SeekableStream;
use bytes::Bytes;
use futures::{
    ready,
    stream::{Fuse, FusedStream},
    AsyncRead, AsyncReadExt, Stream, StreamExt,
};

pub(crate) struct PartitionedStream {
    inner: Box<dyn SeekableStream>,
    buf: Vec<u8>,
    partition_len: usize,
    buf_offset: usize,
    total_read: usize,
    inner_complete: bool,
}

impl PartitionedStream {
    pub(crate) fn new(inner: Box<dyn SeekableStream>, partition_len: usize) -> Self {
        assert!(partition_len > 0);
        Self {
            buf: vec![0u8; std::cmp::min(partition_len, inner.len())],
            inner,
            partition_len,
            buf_offset: 0,
            total_read: 0,
            inner_complete: false,
        }
    }

    fn take(&mut self) -> Vec<u8> {
        let mut ret = mem::replace(
            &mut self.buf,
            vec![0u8; std::cmp::min(self.partition_len, self.inner.len() - self.total_read)],
        );
        ret.truncate(self.buf_offset);
        self.buf_offset = 0;
        ret
    }
}

impl Stream for PartitionedStream {
    type Item = azure_core::Result<Bytes>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            if this.inner_complete || this.buf_offset >= this.buf.len() {
                let ret = this.take();
                return if ret.is_empty() {
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(Ok(Bytes::from(ret))))
                };
            } else {
                match ready!(pin!(&mut this.inner).poll_read(cx, &mut this.buf[this.buf_offset..]))
                {
                    Ok(bytes_read) => {
                        this.buf_offset += bytes_read;
                        this.total_read += bytes_read;
                        this.inner_complete = bytes_read == 0;
                    }
                    Err(e) => {
                        return Poll::Ready(Some(Err(e.into())));
                    }
                }
            }
        }
    }
}

impl FusedStream for PartitionedStream {
    fn is_terminated(&self) -> bool {
        self.inner_complete && self.buf.is_empty()
    }
}
