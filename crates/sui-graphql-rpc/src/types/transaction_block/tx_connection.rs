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
    edges: Vec<TransactionBlockEdge>,
    page_info: PageInfo,
}

#[Object]
impl TransactionBlockConnection {
    async fn edges(&self) -> &[TransactionBlockEdge] {
        &self.edges
    }

    async fn page_info(&self) -> PageInfo {
        let start_cursor = self
            .page_info
            .start_cursor
            .clone()
            .or_else(|| self.edges.first().map(|edge| edge.cursor.clone()));

        let end_cursor = self
            .page_info
            .end_cursor
            .clone()
            .or_else(|| self.edges.last().map(|edge| edge.cursor.clone()));

        PageInfo {
            has_previous_page: self.page_info.has_previous_page,
            has_next_page: self.page_info.has_next_page,
            start_cursor,
            end_cursor,
        }
    }
}

impl TransactionBlockConnection {
    pub(crate) fn new(
        has_previous_page: bool,
        has_next_page: bool,
        start_cursor: Option<String>,
        end_cursor: Option<String>,
    ) -> Self {
        let page_info = PageInfo {
            has_previous_page,
            has_next_page,
            start_cursor,
            end_cursor,
        };
        Self {
            edges: Vec::new(),
            page_info,
        }
    }
}
