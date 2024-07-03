// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::authority::AuthorityState;
use crate::authority_aggregator::AuthorityAggregator;
use crate::authority_client::AuthorityAPI;
use std::sync::{Arc, Weak};
use std::time::Duration;
use sui_types::transaction::VerifiedSignedTransaction;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

/// Only wake up the transaction finalization task for a given transaction
/// after 15 mins of seeing it. This gives plenty of time for the transaction
/// to become final in the normal way.
const TX_FINALIZATION_DELAY: Duration = Duration::from_secs(15 * 60);
/// If a transaction can not be finalized within 10 mins of being woken up, give up.
const FINALIZATION_TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// The `ValidatorTxFinalizer` is responsible for finalizing transactions that
/// have been signed by the validator. It does this by waiting for a delay
/// after the transaction has been signed, and then attempting to finalize
/// the transaction if it has not yet been done by a fullnode.
pub struct ValidatorTxFinalizer<C: Clone> {
    // TODO: Add metrics.
    authority: Weak<AuthorityState>,
    agg: Weak<AuthorityAggregator<C>>,
    exit: CancellationToken,
    tx_finalization_delay: Duration,
    finalization_timeout: Duration,
}

impl<C: Clone> ValidatorTxFinalizer<C> {
    pub fn new(
        authority: Weak<AuthorityState>,
        agg: Weak<AuthorityAggregator<C>>,
        exit: CancellationToken,
    ) -> Self {
        Self {
            authority,
            agg,
            exit,
            tx_finalization_delay: TX_FINALIZATION_DELAY,
            finalization_timeout: FINALIZATION_TIMEOUT,
        }
    }

    #[cfg(test)]
    pub fn new_for_testing(
        authority: Weak<AuthorityState>,
        agg: Weak<AuthorityAggregator<C>>,
        exit: CancellationToken,
        tx_finalization_delay: Duration,
        finalization_timeout: Duration,
    ) -> Self {
        Self {
            authority,
            agg,
            exit,
            tx_finalization_delay,
            finalization_timeout,
        }
    }
}

impl<C> ValidatorTxFinalizer<C>
where
    C: Clone + AuthorityAPI + Send + Sync + 'static,
{
    pub async fn track_signed_transaction(&self, tx: VerifiedSignedTransaction) {
        let authority = self.authority.clone();
        let agg = self.agg.clone();
        let exit = self.exit.clone();
        let tx_finalization_delay = self.tx_finalization_delay;
        let finalization_timeout = self.finalization_timeout;
        tokio::spawn(async move {
            tokio::select! {
                _ = exit.cancelled() => {},
                _ = tokio::time::sleep(tx_finalization_delay) => {
                    if let (Some(authority), Some(agg)) = (authority.upgrade(), agg.upgrade()) {
                        let tx_digest = *tx.digest();
                        tokio::select! {
                            res = tokio::time::timeout(finalization_timeout, Self::finalize_signed_transaction(authority, agg, tx)) => {
                                match res {
                                    Ok(Ok(did_run)) => {
                                        if did_run {
                                            debug!(?tx_digest, "Successfully finalized transaction");
                                        }
                                    },
                                    Ok(Err(err)) => {
                                        error!(?tx_digest, ?err, "Failed to finalize transaction");
                                    }
                                    Err(_) => {
                                        error!(?tx_digest, "Timed out finalizing transaction");
                                    }
                                }
                            },
                            _ = exit.cancelled() => {},
                        }
                    }
                }
            }
        });
    }

    async fn finalize_signed_transaction(
        authority: Arc<AuthorityState>,
        agg: Arc<AuthorityAggregator<C>>,
        tx: VerifiedSignedTransaction,
    ) -> anyhow::Result<bool> {
        let epoch_store = authority.load_epoch_store_one_call_per_task();
        if tx.auth_sig().epoch != epoch_store.epoch() {
            return Ok(false);
        }
        if authority.is_tx_already_executed(tx.digest())? {
            return Ok(false);
        }
        agg.execute_transaction_block(&tx.into_unsigned().inner(), None)
            .await?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use crate::authority::test_authority_builder::TestAuthorityBuilder;
    use crate::authority::AuthorityState;
    use crate::authority_aggregator::AuthorityAggregator;
    use crate::authority_client::AuthorityAPI;
    use crate::validator_tx_finalizer::ValidatorTxFinalizer;
    use async_trait::async_trait;
    use prometheus::default_registry;
    use std::collections::{BTreeMap, HashMap};
    use std::iter;
    use std::net::SocketAddr;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use sui_swarm_config::network_config_builder::ConfigBuilder;
    use sui_test_transaction_builder::TestTransactionBuilder;
    use sui_types::base_types::{AuthorityName, ObjectID, SuiAddress};
    use sui_types::committee::{CommitteeTrait, StakeUnit};
    use sui_types::crypto::{get_account_key_pair, AccountKeyPair};
    use sui_types::effects::TransactionEffectsAPI;
    use sui_types::effects::TransactionEvents;
    use sui_types::error::SuiError;
    use sui_types::executable_transaction::VerifiedExecutableTransaction;
    use sui_types::messages_checkpoint::{
        CheckpointRequest, CheckpointRequestV2, CheckpointResponse, CheckpointResponseV2,
    };
    use sui_types::messages_grpc::{
        HandleCertificateRequestV3, HandleCertificateResponseV2, HandleCertificateResponseV3,
        HandleSoftBundleCertificatesRequestV3, HandleSoftBundleCertificatesResponseV3,
        HandleTransactionResponse, ObjectInfoRequest, ObjectInfoResponse, SystemStateRequest,
        TransactionInfoRequest, TransactionInfoResponse,
    };
    use sui_types::object::Object;
    use sui_types::sui_system_state::SuiSystemState;
    use sui_types::transaction::{
        CertifiedTransaction, SignedTransaction, Transaction, VerifiedCertificate,
        VerifiedSignedTransaction, VerifiedTransaction,
    };
    use sui_types::utils::to_sender_signed_transaction;
    use tokio_util::sync::CancellationToken;

    #[derive(Clone)]
    struct MockAuthorityClient {
        authority: Arc<AuthorityState>,
    }

    #[async_trait]
    impl AuthorityAPI for MockAuthorityClient {
        async fn handle_transaction(
            &self,
            transaction: Transaction,
            _client_addr: Option<SocketAddr>,
        ) -> Result<HandleTransactionResponse, SuiError> {
            let epoch_store = self.authority.epoch_store_for_testing();
            self.authority
                .handle_transaction(
                    &epoch_store,
                    VerifiedTransaction::new_unchecked(transaction),
                )
                .await
        }

        async fn handle_certificate_v2(
            &self,
            certificate: CertifiedTransaction,
            _client_addr: Option<SocketAddr>,
        ) -> Result<HandleCertificateResponseV2, SuiError> {
            let epoch_store = self.authority.epoch_store_for_testing();
            let (effects, _) = self
                .authority
                .try_execute_immediately(
                    &VerifiedExecutableTransaction::new_from_certificate(
                        VerifiedCertificate::new_unchecked(certificate),
                    ),
                    None,
                    &epoch_store,
                )
                .await?;
            let events = match effects.events_digest() {
                None => TransactionEvents::default(),
                Some(digest) => self.authority.get_transaction_events(digest)?,
            };
            let signed_effects = self
                .authority
                .sign_effects(effects, &epoch_store)?
                .into_inner();
            Ok(HandleCertificateResponseV2 {
                signed_effects,
                events,
                fastpath_input_objects: vec![],
            })
        }

        async fn handle_certificate_v3(
            &self,
            _request: HandleCertificateRequestV3,
            _client_addr: Option<SocketAddr>,
        ) -> Result<HandleCertificateResponseV3, SuiError> {
            unimplemented!()
        }

        async fn handle_soft_bundle_certificates_v3(
            &self,
            _request: HandleSoftBundleCertificatesRequestV3,
            _client_addr: Option<SocketAddr>,
        ) -> Result<HandleSoftBundleCertificatesResponseV3, SuiError> {
            unimplemented!()
        }

        async fn handle_object_info_request(
            &self,
            _request: ObjectInfoRequest,
        ) -> Result<ObjectInfoResponse, SuiError> {
            unimplemented!()
        }

        async fn handle_transaction_info_request(
            &self,
            _request: TransactionInfoRequest,
        ) -> Result<TransactionInfoResponse, SuiError> {
            unimplemented!()
        }

        async fn handle_checkpoint(
            &self,
            _request: CheckpointRequest,
        ) -> Result<CheckpointResponse, SuiError> {
            unimplemented!()
        }

        async fn handle_checkpoint_v2(
            &self,
            _request: CheckpointRequestV2,
        ) -> Result<CheckpointResponseV2, SuiError> {
            unimplemented!()
        }

        async fn handle_system_state_object(
            &self,
            _request: SystemStateRequest,
        ) -> Result<SuiSystemState, SuiError> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn test_validator_tx_finalizer_basic_flow() {
        let (sender, keypair) = get_account_key_pair();
        let gas_object = Object::with_owner_for_testing(sender);
        let gas_object_id = gas_object.id();
        let (states, auth_agg, clients) = create_validators(gas_object).await;
        let finalizer1 = ValidatorTxFinalizer::new_for_testing(
            Arc::downgrade(&states[0]),
            Arc::downgrade(&auth_agg),
            CancellationToken::new(),
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(60),
        );
        let signed_tx = create_tx(&clients, &states[0], sender, &keypair, gas_object_id).await;
        let tx_digest = *signed_tx.digest();
        finalizer1
            .track_signed_transaction(VerifiedSignedTransaction::new_unchecked(signed_tx))
            .await;
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        let executed_weight: StakeUnit = clients
            .iter()
            .filter_map(|(name, client)| {
                client
                    .authority
                    .is_tx_already_executed(&tx_digest)
                    .unwrap()
                    .then_some(auth_agg.committee.weight(name))
            })
            .sum();
        assert!(executed_weight >= auth_agg.committee.quorum_threshold());
    }

    #[tokio::test]
    async fn test_validator_tx_finalizer_new_epoch() {
        let (sender, keypair) = get_account_key_pair();
        let gas_object = Object::with_owner_for_testing(sender);
        let gas_object_id = gas_object.id();
        let (states, auth_agg, clients) = create_validators(gas_object).await;
        let finalizer1 = ValidatorTxFinalizer::new_for_testing(
            Arc::downgrade(&states[0]),
            Arc::downgrade(&auth_agg),
            CancellationToken::new(),
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(60),
        );
        let signed_tx = create_tx(&clients, &states[0], sender, &keypair, gas_object_id).await;
        let tx_digest = *signed_tx.digest();
        for state in states.iter() {
            state.reconfigure_for_testing().await;
        }
        finalizer1
            .track_signed_transaction(VerifiedSignedTransaction::new_unchecked(signed_tx))
            .await;
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        assert!(clients
            .iter()
            .all(|(_, client)| !client.authority.is_tx_already_executed(&tx_digest).unwrap()));
    }

    #[tokio::test]
    async fn test_validator_tx_finalizer_cancel() {
        let (sender, keypair) = get_account_key_pair();
        let gas_object = Object::with_owner_for_testing(sender);
        let gas_object_id = gas_object.id();
        let (states, auth_agg, clients) = create_validators(gas_object).await;
        let exit = CancellationToken::new();
        let finalizer1 = ValidatorTxFinalizer::new_for_testing(
            Arc::downgrade(&states[0]),
            Arc::downgrade(&auth_agg),
            exit.clone(),
            std::time::Duration::from_secs(10),
            std::time::Duration::from_secs(60),
        );
        let signed_tx = create_tx(&clients, &states[0], sender, &keypair, gas_object_id).await;
        let tx_digest = *signed_tx.digest();
        finalizer1
            .track_signed_transaction(VerifiedSignedTransaction::new_unchecked(signed_tx))
            .await;
        exit.cancel();
        tokio::time::sleep(std::time::Duration::from_secs(20)).await;
        assert!(clients
            .iter()
            .all(|(_, client)| !client.authority.is_tx_already_executed(&tx_digest).unwrap()));
    }

    async fn create_validators(
        gas_object: Object,
    ) -> (
        Vec<Arc<AuthorityState>>,
        Arc<AuthorityAggregator<MockAuthorityClient>>,
        BTreeMap<AuthorityName, MockAuthorityClient>,
    ) {
        let network_config = ConfigBuilder::new_with_temp_dir()
            .committee_size(NonZeroUsize::new(4).unwrap())
            .with_objects(iter::once(gas_object))
            .build();
        let mut authority_states = vec![];
        for idx in 0..4 {
            let state = TestAuthorityBuilder::new()
                .with_network_config(&network_config, idx)
                .build()
                .await;
            authority_states.push(state);
        }
        let clients: BTreeMap<_, _> = authority_states
            .iter()
            .map(|state| {
                (
                    state.name,
                    MockAuthorityClient {
                        authority: state.clone(),
                    },
                )
            })
            .collect();
        let auth_agg = AuthorityAggregator::new(
            network_config.committee_with_network().committee,
            authority_states[0].clone_committee_store(),
            clients.clone(),
            default_registry(),
            Arc::new(HashMap::new()),
        );
        (authority_states, Arc::new(auth_agg), clients)
    }

    async fn create_tx(
        clients: &BTreeMap<AuthorityName, MockAuthorityClient>,
        state: &Arc<AuthorityState>,
        sender: SuiAddress,
        keypair: &AccountKeyPair,
        gas_object_id: ObjectID,
    ) -> SignedTransaction {
        let gas_object_ref = state
            .get_object(&gas_object_id)
            .await
            .unwrap()
            .unwrap()
            .compute_object_reference();
        let tx_data = TestTransactionBuilder::new(
            sender,
            gas_object_ref,
            state.reference_gas_price_for_testing().unwrap(),
        )
        .transfer_sui(None, sender)
        .build();
        let tx = to_sender_signed_transaction(tx_data, keypair);
        let response = clients
            .get(&state.name)
            .unwrap()
            .handle_transaction(tx.clone(), None)
            .await
            .unwrap();
        SignedTransaction::new_from_data_and_sig(
            tx.into_data(),
            response.status.into_signed_for_testing(),
        )
    }
}
