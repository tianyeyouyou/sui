// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use diesel::{
    backend::Backend,
    deserialize::{self, FromSql, Queryable, QueryableByName},
    row::NamedRow,
};
use sui_indexer::schema::checkpoints;

use crate::{
    max_option, min_option, query, raw_query::RawQuery,
    types::transaction_block::TransactionBlockFilter,
};

#[derive(Clone, Debug)]
pub(crate) struct TxBounds {
    pub lo: i64,
    pub hi: i64,
}

/// `sql_query` raw queries require `QueryableByName`. The default implementation looks for a table
/// based on the struct name, and it also expects the struct's fields to reflect the table's
/// columns. We can override this behavior by implementing `QueryableByName` for our struct. For
/// `TxBounds`, its fields are derived from `checkpoints`, so we can't leverage the default
/// implementation directly.
impl<DB> QueryableByName<DB> for TxBounds
where
    DB: Backend,
    i64: FromSql<diesel::sql_types::BigInt, DB>,
{
    fn build<'a>(row: &impl NamedRow<'a, DB>) -> deserialize::Result<Self> {
        let lo = NamedRow::get::<diesel::sql_types::BigInt, _>(row, "lo")?;
        let hi = NamedRow::get::<diesel::sql_types::BigInt, _>(row, "hi")?;

        Ok(Self { lo, hi })
    }
}

impl TransactionBlockFilter {
    /// Anchor the lower and upper checkpoint bounds if provided from the filter, otherwise default
    /// the lower bound to 0 and the upper bound to `checkpoint_viewed_at`. Increment `after` by 1
    /// so that we can uniformly select the `min_tx_sequence_number` for the lower bound. Similarly,
    /// decrement `before` by 1 so that we can uniformly select the `max_tx_sequence_number`. In
    /// other words, the range consists of all transactions from the smallest tx_sequence_number in
    /// lo_cp to the max tx_sequence_number in hi_cp.
    pub(crate) fn cp_bounds(&self, checkpoint_viewed_at: u64) -> (u64, u64) {
        let lo_cp = max_option!(
            self.after_checkpoint.map(|x| x.saturating_add(1)),
            self.at_checkpoint
        )
        .unwrap_or(0);
        let hi_cp = min_option!(
            self.before_checkpoint.map(|x| x.saturating_sub(1)),
            self.at_checkpoint
        )
        .unwrap_or(checkpoint_viewed_at);

        (lo_cp, hi_cp)
    }
}

/// Constructs a query that selects the first tx_sequence_number of lo_cp and the last
/// tx_sequence_number of hi_cp. The first tx_sequence_number of lo_cp is the
/// `network_total_transactions` of lo_cp - 1, and the last tx_sequence_number is the
/// `network_total_transactions` - 1 of `hi_cp`.
pub(crate) fn tx_bounds_query(lo_cp: u64, hi_cp: u64) -> RawQuery {
    let lo = match lo_cp {
        0 => query!("SELECT 0"),
        _ => query!(format!(
            r#"SELECT network_total_transactions
            FROM checkpoints
            WHERE sequence_number = {}"#,
            lo_cp.saturating_sub(1)
        )),
    };

    let hi = query!(format!(
        r#"SELECT network_total_transactions - 1
        FROM checkpoints
        WHERE sequence_number = {}"#,
        hi_cp
    ));

    query!(
        "SELECT CAST(({}) AS BIGINT) AS lo, CAST(({}) AS BIGINT) AS hi",
        lo,
        hi
    )
}

/// Determines the maximum value in an arbitrary number of Option<u64>.
#[macro_export]
macro_rules! max_option {
    ($($x:expr),+ $(,)?) => {{
        [$($x),*].iter()
            .filter_map(|&x| x)
            .max()
    }};
}

/// Determines the minimum value in an arbitrary number of Option<u64>.
#[macro_export]
macro_rules! min_option {
    ($($x:expr),+ $(,)?) => {{
        [$($x),*].iter()
            .filter_map(|&x| x)
            .min()
    }};
}

/**
 * determine the cp lower and upper bound, construct cte to fetch corresponding tx_sequence_number
 * need to reconcile this with the `after` and `before` cursors
 * and also the `scan_limit` on `tx_sequence_number`
 *
 * What are all possibilities?
 * lo_cp
 * hi_cp
 * before
 * after
 * scan_limit
 *
 * _cp and before/after can be applied together
 * how does scan_limit interact with the above?
 * can we just tack it on further?
 */
pub struct Paginate {
    pub(crate) after: Option<u64>,
    pub(crate) before: Option<u64>,
    pub(crate) scan_limit: Option<u64>,
}

/// Determines the lower checkpoint bound for a transaction block query. Increment `after` by 1 so
/// that we can uniformly select the `min_tx_sequence_number` for the lower bound. Assumes that
/// `after_cp` is less than `at_cp`; thus if both are provided, we only consider `at_cp`.
pub(crate) fn cp_bounds(
    after_cp: Option<u64>,
    at_cp: Option<u64>,
    before_cp: Option<u64>,
    after_cursor: Option<u64>,
) -> Option<u64> {
    let mut selects = Vec::new();

    if let Some(at_cp) = at_cp {
        selects.push(format!(
            "SELECT network_total_transactions FROM checkpoints WHERE sequence_number = {}",
            at_cp.saturating_sub(1)
        ));
    } else {
        if let Some(after_cp) = after_cp {
            selects.push(format!(
                "SELECT network_total_transactions FROM checkpoints WHERE sequence_number = {}",
                after_cp
            ));
        }
    }
    // SELECT GREATEST(after_cursor, (select network_total_transactions from checkpoint where cp = after_cp) + 1, (select network_total_transactions from checkpoint where cp = before_cp) - scan_limit)
    // lower bound is the greatest among
    // after_cursor
    // network_total_transactions from after_cp
    // network_total_transactions from before_cp - scan_limit
    max_option!(after_cp.map(|x| x.saturating_add(1)), at_cp)
}
