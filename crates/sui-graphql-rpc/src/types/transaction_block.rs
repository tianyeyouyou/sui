// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_graphql::{
    connection::{Connection, CursorType, Edge},
    dataloader::Loader,
    *,
};
use diesel::{
    deserialize::{Queryable, QueryableByName},
    ExpressionMethods, JoinOnDsl, OptionalExtension, QueryDsl, Selectable, SelectableHelper,
};
use fastcrypto::encoding::{Base58, Encoding};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use sui_indexer::{
    models::transactions::StoredTransaction,
    schema::{amnn_0_hybrid_cp_tx, amnn_0_hybrid_transactions, transactions, tx_digests},
};
use sui_types::{
    base_types::SuiAddress as NativeSuiAddress,
    effects::TransactionEffects as NativeTransactionEffects,
    event::Event as NativeEvent,
    message_envelope::Message,
    transaction::{
        SenderSignedData as NativeSenderSignedData, TransactionData as NativeTransactionData,
        TransactionDataAPI, TransactionExpiration,
    },
};

use crate::{
    consistency::Checkpointed,
    data::{self, DataLoader, Db, DbConnection, QueryExecutor},
    error::Error,
    filter, query,
    raw_query::RawQuery,
    server::watermark_task::Watermark,
    tx_lookups::{
        select_changed, select_fun, select_ids, select_input, select_kind, select_mod, select_pkg,
        select_recipient, select_sender, select_tx, TxLookupBound,
    },
    types::{intersect, type_filter::ModuleFilter},
};

use super::{
    address::Address,
    base64::Base64,
    cursor::{self, Page, Paginated, RawPaginated, Target},
    digest::Digest,
    epoch::Epoch,
    gas::GasInput,
    sui_address::SuiAddress,
    transaction_block_effects::{TransactionBlockEffects, TransactionBlockEffectsKind},
    transaction_block_kind::TransactionBlockKind,
    type_filter::FqNameFilter,
};

/// Wraps the actual transaction block data with the checkpoint sequence number at which the data
/// was viewed, for consistent results on paginating through and resolving nested types.
#[derive(Clone, Debug)]
pub(crate) struct TransactionBlock {
    pub inner: TransactionBlockInner,
    /// The checkpoint sequence number this was viewed at.
    pub checkpoint_viewed_at: u64,
}

#[derive(Clone, Debug)]
pub(crate) enum TransactionBlockInner {
    /// A transaction block that has been indexed and stored in the database,
    /// containing all information that the other two variants have, and more.
    Stored {
        stored_tx: StoredTransaction,
        native: NativeSenderSignedData,
    },
    /// A transaction block that has been executed via executeTransactionBlock
    /// but not yet indexed.
    Executed {
        tx_data: NativeSenderSignedData,
        effects: NativeTransactionEffects,
        events: Vec<NativeEvent>,
    },
    /// A transaction block that has been executed via dryRunTransactionBlock.
    /// This variant also does not return signatures or digest since only `NativeTransactionData` is present.
    DryRun {
        tx_data: NativeTransactionData,
        effects: NativeTransactionEffects,
        events: Vec<NativeEvent>,
    },
}

/// An input filter selecting for either system or programmable transactions.
#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) enum TransactionBlockKindInput {
    /// A system transaction can be one of several types of transactions.
    /// See [unions/transaction-block-kind] for more details.
    SystemTx = 0,
    /// A user submitted transaction block.
    ProgrammableTx = 1,
}

#[derive(InputObject, Debug, Default, Clone)]
pub(crate) struct TransactionBlockFilter {
    pub function: Option<FqNameFilter>,

    /// An input filter selecting for either system or programmable transactions.
    pub kind: Option<TransactionBlockKindInput>,
    pub after_checkpoint: Option<u64>,
    pub at_checkpoint: Option<u64>,
    pub before_checkpoint: Option<u64>,

    pub sign_address: Option<SuiAddress>,
    pub recv_address: Option<SuiAddress>,

    pub input_object: Option<SuiAddress>,
    pub changed_object: Option<SuiAddress>,

    pub transaction_ids: Option<Vec<Digest>>,
}

pub(crate) type Cursor = cursor::JsonCursor<TransactionBlockCursor>;
type Query<ST, GB> = data::Query<ST, transactions::table, GB>;

/// The cursor returned for each `TransactionBlock` in a connection's page of results. The
/// `checkpoint_viewed_at` will set the consistent upper bound for subsequent queries made on this
/// cursor.
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
pub(crate) struct TransactionBlockCursor {
    /// The checkpoint sequence number this was viewed at.
    #[serde(rename = "c")]
    pub checkpoint_viewed_at: u64,
    #[serde(rename = "t")]
    pub tx_sequence_number: u64,
    /// The checkpoint sequence number when the transaction was finalized.
    #[serde(rename = "tc")]
    pub tx_checkpoint_number: u64,
}

/// DataLoader key for fetching a `TransactionBlock` by its digest, optionally constrained by a
/// consistency cursor.
#[derive(Copy, Clone, Hash, Eq, PartialEq, Debug)]
struct DigestKey {
    pub digest: Digest,
    pub checkpoint_viewed_at: u64,
}

#[derive(Clone, Debug, Queryable, QueryableByName, Selectable)]
#[diesel(table_name = transactions)]
pub struct TxLookup {
    pub tx_sequence_number: i64,
}

#[Object]
impl TransactionBlock {
    /// A 32-byte hash that uniquely identifies the transaction block contents, encoded in Base58.
    /// This serves as a unique id for the block on chain.
    async fn digest(&self) -> Option<String> {
        self.native_signed_data()
            .map(|s| Base58::encode(s.digest()))
    }

    /// The address corresponding to the public key that signed this transaction. System
    /// transactions do not have senders.
    async fn sender(&self) -> Option<Address> {
        let sender = self.native().sender();

        (sender != NativeSuiAddress::ZERO).then(|| Address {
            address: SuiAddress::from(sender),
            checkpoint_viewed_at: self.checkpoint_viewed_at,
        })
    }

    /// The gas input field provides information on what objects were used as gas as well as the
    /// owner of the gas object(s) and information on the gas price and budget.
    ///
    /// If the owner of the gas object(s) is not the same as the sender, the transaction block is a
    /// sponsored transaction block.
    async fn gas_input(&self, ctx: &Context<'_>) -> Option<GasInput> {
        let checkpoint_viewed_at = if matches!(self.inner, TransactionBlockInner::Stored { .. }) {
            self.checkpoint_viewed_at
        } else {
            // Non-stored transactions have a sentinel checkpoint_viewed_at value that generally
            // prevents access to further queries, but inputs should generally be available so try
            // to access them at the high watermark.
            let Watermark { checkpoint, .. } = *ctx.data_unchecked();
            checkpoint
        };

        Some(GasInput::from(
            self.native().gas_data(),
            checkpoint_viewed_at,
        ))
    }

    /// The type of this transaction as well as the commands and/or parameters comprising the
    /// transaction of this kind.
    async fn kind(&self) -> Option<TransactionBlockKind> {
        Some(TransactionBlockKind::from(
            self.native().kind().clone(),
            self.checkpoint_viewed_at,
        ))
    }

    /// A list of all signatures, Base64-encoded, from senders, and potentially the gas owner if
    /// this is a sponsored transaction.
    async fn signatures(&self) -> Option<Vec<Base64>> {
        self.native_signed_data().map(|s| {
            s.tx_signatures()
                .iter()
                .map(|sig| Base64::from(sig.as_ref()))
                .collect()
        })
    }

    /// The effects field captures the results to the chain of executing this transaction.
    async fn effects(&self) -> Result<Option<TransactionBlockEffects>> {
        Ok(Some(self.clone().try_into().extend()?))
    }

    /// This field is set by senders of a transaction block. It is an epoch reference that sets a
    /// deadline after which validators will no longer consider the transaction valid. By default,
    /// there is no deadline for when a transaction must execute.
    async fn expiration(&self, ctx: &Context<'_>) -> Result<Option<Epoch>> {
        let TransactionExpiration::Epoch(id) = self.native().expiration() else {
            return Ok(None);
        };

        Epoch::query(ctx, Some(*id), self.checkpoint_viewed_at)
            .await
            .extend()
    }

    /// Serialized form of this transaction's `SenderSignedData`, BCS serialized and Base64 encoded.
    async fn bcs(&self) -> Option<Base64> {
        match &self.inner {
            TransactionBlockInner::Stored { stored_tx, .. } => {
                Some(Base64::from(&stored_tx.raw_transaction))
            }
            TransactionBlockInner::Executed { tx_data, .. } => {
                bcs::to_bytes(&tx_data).ok().map(Base64::from)
            }
            // Dry run transaction does not have signatures so no sender signed data.
            TransactionBlockInner::DryRun { .. } => None,
        }
    }
}

impl TransactionBlock {
    fn native(&self) -> &NativeTransactionData {
        match &self.inner {
            TransactionBlockInner::Stored { native, .. } => native.transaction_data(),
            TransactionBlockInner::Executed { tx_data, .. } => tx_data.transaction_data(),
            TransactionBlockInner::DryRun { tx_data, .. } => tx_data,
        }
    }

    fn native_signed_data(&self) -> Option<&NativeSenderSignedData> {
        match &self.inner {
            TransactionBlockInner::Stored { native, .. } => Some(native),
            TransactionBlockInner::Executed { tx_data, .. } => Some(tx_data),
            TransactionBlockInner::DryRun { .. } => None,
        }
    }

    /// Look up a `TransactionBlock` in the database, by its transaction digest. Treats it as if it
    /// is being viewed at the `checkpoint_viewed_at` (e.g. the state of all relevant addresses will
    /// be at that checkpoint).
    pub(crate) async fn query(
        ctx: &Context<'_>,
        digest: Digest,
        checkpoint_viewed_at: u64,
    ) -> Result<Option<Self>, Error> {
        let DataLoader(loader) = ctx.data_unchecked();
        loader
            .load_one(DigestKey {
                digest,
                checkpoint_viewed_at,
            })
            .await
    }

    /// Look up multiple `TransactionBlock`s by their digests. Returns a map from those digests to
    /// their resulting transaction blocks, for the blocks that could be found. We return a map
    /// because the order of results from the DB is not otherwise guaranteed to match the order that
    /// digests were passed into `multi_query`.
    pub(crate) async fn multi_query(
        ctx: &Context<'_>,
        digests: Vec<Digest>,
        checkpoint_viewed_at: u64,
    ) -> Result<BTreeMap<Digest, Self>, Error> {
        let DataLoader(loader) = ctx.data_unchecked();
        let result = loader
            .load_many(digests.into_iter().map(|digest| DigestKey {
                digest,
                checkpoint_viewed_at,
            }))
            .await?;

        Ok(result.into_iter().map(|(k, v)| (k.digest, v)).collect())
    }

    /// Query the database for a `page` of TransactionBlocks. The page uses `tx_sequence_number` and
    /// `checkpoint_viewed_at` as the cursor, and can optionally be further `filter`-ed.
    ///
    /// The `checkpoint_viewed_at` parameter represents the checkpoint sequence number at which this
    /// page was queried for. Each entity returned in the connection will inherit this checkpoint,
    /// so that when viewing that entity's state, it will be from the reference of this
    /// checkpoint_viewed_at parameter.
    ///
    /// If the `Page<Cursor>` is set, then this function will defer to the `checkpoint_viewed_at` in
    /// the cursor if they are consistent.
    ///
    /// Filters that involve a combination of `recvAddress`, `inputObject`, `changedObject`, and
    /// `function` should provide a value for `within_checkpoints`.
    pub(crate) async fn paginate(
        ctx: &Context<'_>,
        page: Page<Cursor>,
        filter: TransactionBlockFilter,
        checkpoint_viewed_at: u64,
        within_checkpoints: Option<u64>,
    ) -> Result<Connection<String, TransactionBlock>, Error> {
        filter.is_consistent()?;

        let db: &Db = ctx.data_unchecked();

        // Anchor the lower and upper checkpoint bounds on what is provided in the filter. Increment
        // `after` by 1 so that we can uniformly select the `min_tx_sequence_number` for the lower
        // bound. Similarly, decrement `before` by 1 so that we can uniformly select the
        // `max_tx_sequence_number`. In other words, the range consists of all transactions from the
        // smallest tx_sequence_number in lo_cp to the max tx_sequence_number in hi_cp.
        let mut lo_cp = max_option(
            filter.after_checkpoint.map(|x| x.saturating_add(1)),
            filter.at_checkpoint,
        );
        let mut hi_cp = min_option(
            filter.at_checkpoint,
            filter.before_checkpoint.map(|x| x.saturating_sub(1)),
        );

        // If `within_checkpoints` is set, we need to adjust the lower and upper bounds. It is up to
        // the caller of `TransactionBlock::paginate` to determine whether `within_checkpoints` is
        // required.
        if let Some(scan_limit) = within_checkpoints {
            if page.is_from_front() {
                hi_cp = min_option(hi_cp, Some(lo_cp.unwrap_or(0).saturating_add(scan_limit)));
            } else {
                // Similar to the `first` case, `checkpoint_viewed_at` offers a fixed point to
                // determine how many more checkpoints to scan.
                lo_cp = max_option(
                    lo_cp,
                    Some(
                        hi_cp
                            .unwrap_or(checkpoint_viewed_at)
                            .saturating_sub(scan_limit),
                    ),
                );
            }
        }

        use amnn_0_hybrid_cp_tx::dsl as cp;
        use amnn_0_hybrid_transactions::dsl as tx;

        let (prev, next, transactions): (bool, bool, Vec<StoredTransaction>) = db
            .execute_repeatable(move |conn| {

                // Map the lower and upper checkpoint bounds to their respective tx_sequence_number.
                let lb_tx_seq_num = match lo_cp {
                    Some(lb_value) => {
                        let sequence_number: Option<i64> = conn
                            .first(move || {
                                cp::amnn_0_hybrid_cp_tx
                                    .select(cp::min_tx_sequence_number)
                                    .filter(cp::checkpoint_sequence_number.eq(lb_value as i64))
                            })
                            .optional()?;

                        match sequence_number {
                            Some(sequence_number) => Some(sequence_number as u64),
                            None => return Ok::<_, diesel::result::Error>((false, false, vec![])),
                        }
                    }
                    None => None,
                };
                let ub_tx_seq_num = match hi_cp {
                    Some(ub_value) => {
                        let sequence_number: Option<i64> = conn
                            .first(move || {
                                cp::amnn_0_hybrid_cp_tx
                                    .select(cp::max_tx_sequence_number)
                                    .filter(cp::checkpoint_sequence_number.eq(ub_value as i64))
                            })
                            .optional()?;

                        match sequence_number {
                            Some(sequence_number) => Some(sequence_number as u64),
                            None => return Ok::<_, diesel::result::Error>((false, false, vec![])),
                        }
                    }
                    None => None,
                };

                let before_cursor = page.before().map(|c| c.tx_sequence_number);
                let after_cursor = page.after().map(|c| c.tx_sequence_number);

                let lo = max_option(lb_tx_seq_num, after_cursor);
                let hi = min_option(ub_tx_seq_num, before_cursor);

                let sender = filter.sign_address;

                // if tx_digests is specified, we can use that to further filter each subquery
                // wonder if we can drop the `within_checkpoints` requirement if `transaction_ids`
                // is specified?

                let mut bound = TxLookupBound::from_range((lo, hi));

                if let Some(txs) = &filter.transaction_ids {
                    let transaction_ids: Vec<TxLookup> = conn.results(move || select_ids(txs, &bound).into_boxed())?;
                    bound = TxLookupBound::from_set(transaction_ids.into_iter().map(|tx| tx.tx_sequence_number as u64).collect());
                }

                let mut subqueries = vec![];

                if let Some(f) = &filter.function {
                    subqueries.push(match f {
                        FqNameFilter::ByModule(filter) => match filter {
                            ModuleFilter::ByPackage(p) => {
                                (select_pkg(p, sender, &bound), "tx_calls_pkg")
                            }
                            ModuleFilter::ByModule(p, m) => {
                                (select_mod(p, m.clone(), sender, &bound), "tx_calls_mod")
                            }
                        },
                        FqNameFilter::ByFqName(p, m, n) => (
                            select_fun(p, m.clone(), n.clone(), sender, &bound),
                            "tx_calls_fun",
                        ),
                    });
                }
                if let Some(kind) = &filter.kind {
                    subqueries.push((select_kind(kind.clone(), &bound), "tx_kinds"));
                    if let Some(sender) = &sender {
                        subqueries.push((select_sender(sender, &bound), "tx_senders"));
                    }
                }
                if let Some(recv) = &filter.recv_address {
                    subqueries.push((select_recipient(recv, sender, &bound), "tx_recipients"));
                }
                if let Some(input) = &filter.input_object {
                    subqueries.push((select_input(input, sender, &bound), "tx_input_objects"));
                }
                if let Some(changed) = &filter.changed_object {
                    subqueries.push((
                        select_changed(changed, sender, &bound),
                        "tx_changed_objects",
                    ));
                }

                if filter.num_filters() == 0 {
                    // We don't need to explicitly specify sender unless it's the only filter
                    if let Some(sender) = &sender {
                        subqueries.push((select_sender(sender, &bound), "tx_senders"));
                    }
                    // And if there are no filters at all, we can operate directly on the main table
                    else {
                        subqueries.push((
                            select_tx(None, &bound, "amnn_0_hybrid_transactions"),
                            "transactions",
                        ));
                    }
                }

                // Finally, join the array of subqueries into a single query that looks like: SELECT
                // tx_sequence_number FROM (SELECT tx_sequence_number FROM table0 WHERE ...) AS
                // table0 INNER JOIN (SELECT ...) AS table1 USING (tx_sequence_number) ORDER BY
                // tx_sequence_number ASC LIMIT 52;

                // There should be at least one subquery even if no filters are specified - the
                // query against the base table. Otherwise the array will contain subqueries that
                // hit lookup tables.
                let mut subquery = subqueries.pop().unwrap().0;

                if !subqueries.is_empty() {
                    subquery = query!(
                        "SELECT tx_sequence_number FROM ({}) AS initial",
                        subquery
                    );
                    while let Some((subselect, alias)) = subqueries.pop() {
                        subquery =
                            query!("{} INNER JOIN {} USING (tx_sequence_number)", subquery, aliased => (subselect, alias));
                    }
                }

                // Issue the query to fetch the set of `tx_sequence_number` that will then be used
                // to fetch remaining contents from the `transactions` table.

                // TODO (wlmyng): this typing is a bit hacky, and we likely don't need it
                println!("Submitting the query now");
                let (prev, next, results) =
                    page.paginate_raw_query::<TxLookup>(conn, checkpoint_viewed_at, subquery)?;

                let tx_sequence_numbers = results
                    .into_iter()
                    .map(|x| x.tx_sequence_number)
                    .collect::<Vec<i64>>();

                // then just do a multi-get
                let transactions = conn.results(move || {
                    tx::amnn_0_hybrid_transactions
                        .filter(tx::tx_sequence_number.eq_any(tx_sequence_numbers.clone()))
                })?;
                Ok::<_, diesel::result::Error>((prev, next, transactions))
            })
            .await?;

        let mut conn = Connection::new(prev, next);

        for stored in transactions {
            let cursor = stored.cursor(checkpoint_viewed_at).encode_cursor();
            let inner = TransactionBlockInner::try_from(stored)?;
            let transaction = TransactionBlock {
                inner,
                checkpoint_viewed_at,
            };
            conn.edges.push(Edge::new(cursor, transaction));
        }

        Ok(conn)
    }
}

impl TransactionBlockFilter {
    /// Try to create a filter whose results are the intersection of transaction blocks in `self`'s
    /// results and transaction blocks in `other`'s results. This may not be possible if the
    /// resulting filter is inconsistent in some way (e.g. a filter that requires one field to be
    /// two different values simultaneously).
    pub(crate) fn intersect(self, other: Self) -> Option<Self> {
        macro_rules! intersect {
            ($field:ident, $body:expr) => {
                intersect::field(self.$field, other.$field, $body)
            };
        }

        Some(Self {
            function: intersect!(function, FqNameFilter::intersect)?,
            kind: intersect!(kind, intersect::by_eq)?,

            after_checkpoint: intersect!(after_checkpoint, intersect::by_max)?,
            at_checkpoint: intersect!(at_checkpoint, intersect::by_eq)?,
            before_checkpoint: intersect!(before_checkpoint, intersect::by_min)?,

            sign_address: intersect!(sign_address, intersect::by_eq)?,
            recv_address: intersect!(recv_address, intersect::by_eq)?,
            input_object: intersect!(input_object, intersect::by_eq)?,
            changed_object: intersect!(changed_object, intersect::by_eq)?,

            transaction_ids: intersect!(transaction_ids, |a, b| {
                let a = BTreeSet::from_iter(a.into_iter());
                let b = BTreeSet::from_iter(b.into_iter());
                Some(a.intersection(&b).cloned().collect())
            })?,
        })
    }

    /// Returns count of filters that would require a `scan_limit` if used together.
    pub(crate) fn num_filters(&self) -> usize {
        [
            self.recv_address.is_some(),
            self.input_object.is_some(),
            self.changed_object.is_some(),
            self.function.is_some(),
            self.kind.is_some(),
        ]
        .iter()
        .filter(|&is_set| *is_set)
        .count()
    }

    pub(crate) fn is_consistent(&self) -> Result<(), Error> {
        if let Some(before) = self.before_checkpoint {
            if !(before > 0) {
                return Err(Error::Client(
                    "`beforeCheckpoint` must be greater than 0".to_string(),
                ));
            }
        }

        if let (Some(after), Some(before)) = (self.after_checkpoint, self.before_checkpoint) {
            // Because `after` and `before` are both exclusive, they must be at least one apart if
            // both are provided.
            if !(after + 1 < before) {
                return Err(Error::Client(
                    "`afterCheckpoint` must be less than `beforeCheckpoint`".to_string(),
                ));
            }
        }

        if let (Some(after), Some(at)) = (self.after_checkpoint, self.at_checkpoint) {
            if !(after < at) {
                return Err(Error::Client(
                    "`afterCheckpoint` must be less than `atCheckpoint`".to_string(),
                ));
            }
        }

        if let (Some(at), Some(before)) = (self.at_checkpoint, self.before_checkpoint) {
            if !(at < before) {
                return Err(Error::Client(
                    "`atCheckpoint` must be less than `beforeCheckpoint`".to_string(),
                ));
            }
        }

        if let (Some(TransactionBlockKindInput::SystemTx), Some(signer)) =
            (self.kind, self.sign_address)
        {
            if !(signer == SuiAddress::from(NativeSuiAddress::ZERO)) {
                return Err(Error::Client(
                    "System transactions cannot have a sender".to_string(),
                ));
            }
        }

        Ok(())
    }
}

impl Paginated<Cursor> for StoredTransaction {
    type Source = transactions::table;

    fn filter_ge<ST, GB>(cursor: &Cursor, query: Query<ST, GB>) -> Query<ST, GB> {
        query
            .filter(transactions::dsl::tx_sequence_number.ge(cursor.tx_sequence_number as i64))
            .filter(
                transactions::dsl::checkpoint_sequence_number
                    .ge(cursor.tx_checkpoint_number as i64),
            )
    }

    fn filter_le<ST, GB>(cursor: &Cursor, query: Query<ST, GB>) -> Query<ST, GB> {
        query
            .filter(transactions::dsl::tx_sequence_number.le(cursor.tx_sequence_number as i64))
            .filter(
                transactions::dsl::checkpoint_sequence_number
                    .le(cursor.tx_checkpoint_number as i64),
            )
    }

    fn order<ST, GB>(asc: bool, query: Query<ST, GB>) -> Query<ST, GB> {
        use transactions::dsl;
        if asc {
            query.order_by(dsl::tx_sequence_number.asc())
        } else {
            query.order_by(dsl::tx_sequence_number.desc())
        }
    }
}

impl Target<Cursor> for StoredTransaction {
    fn cursor(&self, checkpoint_viewed_at: u64) -> Cursor {
        Cursor::new(TransactionBlockCursor {
            tx_sequence_number: self.tx_sequence_number as u64,
            tx_checkpoint_number: self.checkpoint_sequence_number as u64,
            checkpoint_viewed_at,
        })
    }
}

impl Checkpointed for Cursor {
    fn checkpoint_viewed_at(&self) -> u64 {
        self.checkpoint_viewed_at
    }
}

impl Target<Cursor> for TxLookup {
    fn cursor(&self, checkpoint_viewed_at: u64) -> Cursor {
        Cursor::new(TransactionBlockCursor {
            tx_sequence_number: self.tx_sequence_number as u64,
            tx_checkpoint_number: 0, // TODO (wlmyng)
            checkpoint_viewed_at,
        })
    }
}

impl RawPaginated<Cursor> for TxLookup {
    fn filter_ge(cursor: &Cursor, query: RawQuery) -> RawQuery {
        filter!(
            query,
            format!("tx_sequence_number >= {}", cursor.tx_sequence_number)
        )
    }

    fn filter_le(cursor: &Cursor, query: RawQuery) -> RawQuery {
        filter!(
            query,
            format!("tx_sequence_number <= {}", cursor.tx_sequence_number)
        )
    }

    fn order(asc: bool, query: RawQuery) -> RawQuery {
        if asc {
            query.order_by("tx_sequence_number ASC")
        } else {
            query.order_by("tx_sequence_number DESC")
        }
    }
}

#[async_trait::async_trait]
impl Loader<DigestKey> for Db {
    type Value = TransactionBlock;
    type Error = Error;

    async fn load(
        &self,
        keys: &[DigestKey],
    ) -> Result<HashMap<DigestKey, TransactionBlock>, Error> {
        use transactions::dsl as tx;
        use tx_digests::dsl as ds;

        let digests: Vec<_> = keys.iter().map(|k| k.digest.to_vec()).collect();

        let transactions: Vec<StoredTransaction> = self
            .execute(move |conn| {
                conn.results(move || {
                    let join = ds::tx_sequence_number.eq(tx::tx_sequence_number);

                    tx::transactions
                        .inner_join(ds::tx_digests.on(join))
                        .select(StoredTransaction::as_select())
                        .filter(ds::tx_digest.eq_any(digests.clone()))
                })
            })
            .await
            .map_err(|e| Error::Internal(format!("Failed to fetch transactions: {e}")))?;

        let transaction_digest_to_stored: BTreeMap<_, _> = transactions
            .into_iter()
            .map(|tx| (tx.transaction_digest.clone(), tx))
            .collect();

        let mut results = HashMap::new();
        for key in keys {
            let Some(stored) = transaction_digest_to_stored
                .get(key.digest.as_slice())
                .cloned()
            else {
                continue;
            };

            // Filter by key's checkpoint viewed at here. Doing this in memory because it should be
            // quite rare that this query actually filters something, but encoding it in SQL is
            // complicated.
            if key.checkpoint_viewed_at < stored.checkpoint_sequence_number as u64 {
                continue;
            }

            let inner = TransactionBlockInner::try_from(stored)?;
            results.insert(
                *key,
                TransactionBlock {
                    inner,
                    checkpoint_viewed_at: key.checkpoint_viewed_at,
                },
            );
        }

        Ok(results)
    }
}

impl TryFrom<StoredTransaction> for TransactionBlockInner {
    type Error = Error;

    fn try_from(stored_tx: StoredTransaction) -> Result<Self, Error> {
        let native = bcs::from_bytes(&stored_tx.raw_transaction)
            .map_err(|e| Error::Internal(format!("Error deserializing transaction block: {e}")))?;

        Ok(TransactionBlockInner::Stored { stored_tx, native })
    }
}

impl TryFrom<TransactionBlockEffects> for TransactionBlock {
    type Error = Error;

    fn try_from(effects: TransactionBlockEffects) -> Result<Self, Error> {
        let checkpoint_viewed_at = effects.checkpoint_viewed_at;
        let inner = match effects.kind {
            TransactionBlockEffectsKind::Stored { stored_tx, .. } => {
                TransactionBlockInner::try_from(stored_tx.clone())
            }
            TransactionBlockEffectsKind::Executed {
                tx_data,
                native,
                events,
            } => Ok(TransactionBlockInner::Executed {
                tx_data: tx_data.clone(),
                effects: native.clone(),
                events: events.clone(),
            }),
            TransactionBlockEffectsKind::DryRun {
                tx_data,
                native,
                events,
            } => Ok(TransactionBlockInner::DryRun {
                tx_data: tx_data.clone(),
                effects: native.clone(),
                events: events.clone(),
            }),
        }?;

        Ok(TransactionBlock {
            inner,
            checkpoint_viewed_at,
        })
    }
}

fn max_option(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    match (a, b) {
        (Some(a_val), Some(b_val)) => Some(std::cmp::max(a_val, b_val)),
        (Some(val), None) | (None, Some(val)) => Some(val),
        (None, None) => None,
    }
}

fn min_option(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    match (a, b) {
        (Some(a_val), Some(b_val)) => Some(std::cmp::min(a_val, b_val)),
        (Some(val), None) | (None, Some(val)) => Some(val),
        (None, None) => None,
    }
}
