// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::raw_query::RawQuery;
use crate::types::digest::Digest;
use crate::types::sui_address::SuiAddress;
use crate::types::transaction_block::TransactionBlockKindInput;
use crate::{filter, query};
use std::fmt::{self, Write};

pub(crate) enum TxLookupBound {
    Range((Option<u64>, Option<u64>)),
    Set(Vec<u64>),
}

impl TxLookupBound {
    pub(crate) fn from_range(range: (Option<u64>, Option<u64>)) -> Self {
        TxLookupBound::Range(range)
    }

    pub(crate) fn from_set(ids: Vec<u64>) -> Self {
        TxLookupBound::Set(ids)
    }
}

pub(crate) fn select_tx(sender: Option<SuiAddress>, bound: &TxLookupBound, from: &str) -> RawQuery {
    let mut query = query!(format!("SELECT tx_sequence_number FROM {}", from));

    if let Some(sender) = sender {
        query = filter!(
            query,
            format!("sender = '\\x{}'::bytea", hex::encode(sender.into_vec()))
        );
    }

    match bound {
        TxLookupBound::Range((lo, hi)) => {
            if let Some(lo) = lo {
                query = filter!(query, format!("tx_sequence_number >= {}", lo));
            }

            if let Some(hi) = hi {
                query = filter!(query, format!("tx_sequence_number <= {}", hi));
            }
        }
        TxLookupBound::Set(ids) => {
            let mut inner = String::new();
            let mut prefix = "tx_sequence_number IN (";
            for id in ids {
                write!(&mut inner, "{prefix}{}", id).unwrap();
                prefix = ", ";
            }
            inner.push(')');
            query = filter!(query, inner);
        }
    }

    query
}

pub(crate) fn select_pkg(
    pkg: &SuiAddress,
    sender: Option<SuiAddress>,
    bound: &TxLookupBound,
) -> RawQuery {
    filter!(
        select_tx(sender, bound, "tx_calls_pkg"),
        format!("package = {}", bytea_literal(pkg))
    )
}

pub(crate) fn select_mod(
    pkg: &SuiAddress,
    mod_: String,
    sender: Option<SuiAddress>,
    bound: &TxLookupBound,
) -> RawQuery {
    filter!(
        select_tx(sender, bound, "tx_calls_mod"),
        format!("package = {} and module = {{}}", bytea_literal(pkg)),
        mod_
    )
}

pub(crate) fn select_fun(
    pkg: &SuiAddress,
    mod_: String,
    fun: String,
    sender: Option<SuiAddress>,
    bound: &TxLookupBound,
) -> RawQuery {
    filter!(
        select_tx(sender, bound, "tx_calls_fun"),
        format!(
            "package = {} AND module = {{}} AND func = {{}}",
            bytea_literal(pkg),
        ),
        mod_,
        fun
    )
}

pub(crate) fn select_kind(kind: TransactionBlockKindInput, bound: &TxLookupBound) -> RawQuery {
    filter!(
        select_tx(None, bound, "tx_kinds"),
        format!("tx_kind = {}", kind as i16)
    )
}

pub(crate) fn select_sender(sender: &SuiAddress, bound: &TxLookupBound) -> RawQuery {
    select_tx(Some(*sender), bound, "tx_senders")
}

pub(crate) fn select_recipient(
    recv: &SuiAddress,
    sender: Option<SuiAddress>,
    bound: &TxLookupBound,
) -> RawQuery {
    filter!(
        select_tx(sender, bound, "tx_recipients"),
        format!("recipient = '\\x{}'::bytea", hex::encode(recv.into_vec()))
    )
}

pub(crate) fn select_input(
    input: &SuiAddress,
    sender: Option<SuiAddress>,
    bound: &TxLookupBound,
) -> RawQuery {
    filter!(
        select_tx(sender, bound, "tx_input_objects"),
        format!("object_id = '\\x{}'::bytea", hex::encode(input.into_vec()))
    )
}

pub(crate) fn select_changed(
    changed: &SuiAddress,
    sender: Option<SuiAddress>,
    bound: &TxLookupBound,
) -> RawQuery {
    filter!(
        select_tx(sender, bound, "tx_changed_objects"),
        format!(
            "object_id = '\\x{}'::bytea",
            hex::encode(changed.into_vec())
        )
    )
}

pub(crate) fn select_ids(ids: &Vec<Digest>, bound: &TxLookupBound) -> RawQuery {
    let query = select_tx(None, bound, "tx_digests");
    if ids.is_empty() {
        filter!(query, "1=0")
    } else {
        let mut inner = String::new();
        let mut prefix = "tx_digest IN (";
        for id in ids {
            write!(
                &mut inner,
                "{prefix}'\\x{}'::bytea",
                hex::encode(id.to_vec())
            )
            .unwrap();
            prefix = ", ";
        }
        inner.push(')');
        filter!(query, inner)
    }
}

pub(crate) fn bytea_literal(addr: &SuiAddress) -> impl fmt::Display + '_ {
    struct ByteaLiteral<'a>(&'a [u8]);

    impl fmt::Display for ByteaLiteral<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "'\\x{}'::bytea", hex::encode(self.0))
        }
    }

    ByteaLiteral(addr.as_slice())
}
