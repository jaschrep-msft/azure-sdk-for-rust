mod copy;
mod download;
mod upload;
pub use copy::*;
pub use download::*;
pub use upload::*;

use std::future::Future;

use futures::{Stream, TryStreamExt};

type AzureResult<T> = azure_core::Result<T>;

async fn run_all_with_concurrency_limit<TFut, TErr>(
    mut ops: impl Stream<Item = Result<impl FnOnce() -> TFut, TErr>> + Unpin,
    parallel: usize,
) -> Result<(), TErr>
where
    TFut: Future<Output = Result<(), TErr>>,
{
    let mut running_ops = Vec::with_capacity(parallel);

    // This loop fills running_ops before it lets a single op actually run
    // fine for in-memory source but not for IO streams
    // need else case where fetching next and executing current are either'd
    while let Some(op) = ops.try_next().await? {
        running_ops.push(Box::pin(op()));
        if running_ops.len() >= parallel {
            let result;
            (result, _, running_ops) = futures::future::select_all(running_ops).await;
            result?;
        }
    }
    while !running_ops.is_empty() {
        let result;
        (result, _, running_ops) = futures::future::select_all(running_ops).await;
        result?;
    }
    Ok(())
}

fn div_round_up(left: usize, right: usize) -> usize {
    ((left as f64) / (right as f64)).ceil() as usize
}
