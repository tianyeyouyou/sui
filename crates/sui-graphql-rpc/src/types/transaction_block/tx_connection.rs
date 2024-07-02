// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_graphql::connection::PageInfo;
use async_graphql::{Object, SimpleObject};

use super::TransactionBlock;

#[derive(SimpleObject)]
pub(crate) struct TransactionBlockEdge {
    pub(crate) cursor: String,
    pub(crate) node: TransactionBlock,
}

pub(crate) struct TransactionBlockConnection {
    pub edges: Vec<TransactionBlockEdge>,
    pub has_previous_page: bool,
    pub has_next_page: bool,
    pub start_cursor: Option<String>,
    pub end_cursor: Option<String>,
}

#[Object]
impl TransactionBlockConnection {
    async fn edges(&self) -> &[TransactionBlockEdge] {
        &self.edges
    }

    async fn page_info(&self) -> PageInfo {
        let start_cursor = self
            .start_cursor
            .clone()
            .or_else(|| self.edges.first().map(|edge| edge.cursor.clone()));

        let end_cursor = self
            .end_cursor
            .clone()
            .or_else(|| self.edges.last().map(|edge| edge.cursor.clone()));

        PageInfo {
            has_previous_page: self.has_previous_page,
            has_next_page: self.has_next_page,
            start_cursor,
            end_cursor,
        }
    }
}

impl TransactionBlockConnection {
    pub(crate) fn new(has_previous_page: bool, has_next_page: bool) -> Self {
        Self {
            edges: Vec::new(),
            has_previous_page,
            has_next_page,
            start_cursor: None,
            end_cursor: None,
        }
    }

    pub(crate) fn update_page_info(
        &mut self,
        has_previous_page: bool,
        has_next_page: bool,
        start_cursor: Option<String>,
        end_cursor: Option<String>,
    ) {
        self.has_previous_page = has_previous_page;
        self.has_next_page = has_next_page;
        self.start_cursor = start_cursor;
        self.end_cursor = end_cursor;
    }
}

impl TransactionBlockEdge {
    pub(crate) fn new(cursor: String, node: TransactionBlock) -> Self {
        Self { cursor, node }
    }
}
