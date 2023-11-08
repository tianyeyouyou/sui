use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    net::SocketAddr,
};
use std::{fmt::Debug, path::Path};
use std::{fs, net::IpAddr};
use sui_protocol_config::ProtocolVersion;
use sui_types::transaction::SenderSignedData;
use sui_types::{
    base_types::{ObjectID, ObjectRef, SequenceNumber},
    digests::TransactionDigest,
    effects::TransactionEffects,
    epoch_data::EpochData,
    object::Object,
    storage::{DeleteKind, WriteKind},
    sui_system_state::epoch_start_sui_system_state::EpochStartSystemState,
    transaction::{InputObjectKind, TransactionDataAPI, TransactionKind},
};

pub type UniqueId = u16;

#[derive(Clone, Deserialize, Debug)]
pub struct ServerConfig {
    pub kind: String,
    pub ip_addr: IpAddr,
    pub port: u16,
    pub metrics_address: SocketAddr,
    pub attrs: HashMap<String, String>,
}

pub type GlobalConfig = HashMap<UniqueId, ServerConfig>;

impl ServerConfig {
    pub const BENCHMARK_BASE_PORT: u16 = 1500;

    /// Create a new global config for benchmarking.
    pub fn new_for_benchmark(ips: Vec<IpAddr>) -> GlobalConfig {
        let benchmark_port_offset = ips.len() as u16;
        let mut global_config = GlobalConfig::new();
        for (i, ip) in ips.into_iter().enumerate() {
            let network_port = Self::BENCHMARK_BASE_PORT + i as u16;
            let metrics_port = benchmark_port_offset + network_port;
            let kind = if i == 0 { "SW" } else { "EW" }.to_string();
            let metrics_address = SocketAddr::new(ip, metrics_port);
            let legacy_metrics = SocketAddr::new(ip, benchmark_port_offset + metrics_port);
            let config = ServerConfig {
                kind,
                ip_addr: ip,
                port: network_port,
                metrics_address,
                attrs: [
                    ("metrics-address".to_string(), legacy_metrics.to_string()),
                    ("execute".to_string(), 100.to_string()),
                    ("mode".to_string(), "channel".to_string()),
                    ("duration".to_string(), 300.to_string()),
                ]
                .iter()
                .cloned()
                .collect(),
            };
            global_config.insert(i as u16, config);
        }
        global_config
    }

    /// Load a global config from a file.
    pub fn from_path<P: AsRef<Path>>(path: P) -> GlobalConfig {
        let config_json = fs::read_to_string(path).expect("Failed to read config file");
        serde_json::from_str(&config_json).expect("Failed to parse config file")
    }
}

pub trait Message {
    fn serialize(&self) -> String;
    fn deserialize(string: String) -> Self;
}

impl Message for std::string::String {
    fn serialize(&self) -> String {
        self.to_string()
    }

    fn deserialize(string: String) -> Self {
        string
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NetworkMessage {
    pub src: UniqueId,
    pub dst: UniqueId,
    pub payload: SailfishMessage,
}

// TODO: Maybe serialize directly to bytes, rather than String and then to bytes
// impl<M: Debug + Serialize + DeserializeOwned> NetworkMessage<M> {
//     pub fn serialize(&self) -> String {
//         format!("{}${}${}$\n", self.src, self.dst, self.payload.serialize())
//     }

//     pub fn deserialize(string: String) -> Self {
//         let mut splitted = string.split("$");
//         let src = splitted.next().unwrap().parse().expect(string.as_str());
//         let dst = splitted.next().unwrap().parse().unwrap();
//         let payload = Message::deserialize(splitted.next().unwrap().to_string());
//         NetworkMessage { src, dst, payload }
//     }
// }

#[derive(Serialize, Deserialize, Debug)]
pub enum SailfishMessage {
    // Sequencing Worker <-> Execution Worker
    EpochStart {
        version: ProtocolVersion,
        data: EpochData,
        ref_gas_price: u64,
    },
    EpochEnd {
        new_epoch_start_state: EpochStartSystemState,
    },
    ProposeExec(TransactionWithEffects),
    // Execution Worker <-> Execution Worker
    //LockedExec { tx: TransactionDigest, objects: Vec<(ObjectRef, Object)> },
    LockedExec {
        txid: TransactionDigest,
        objects: Vec<Option<(ObjectRef, Object)>>,
        child_objects: Vec<Option<(ObjectRef, Object)>>,
    },
    MissingObjects {
        txid: TransactionDigest,
        ew: u8,
        missing_objects: HashSet<ObjectID>,
    },
    TxResults {
        txid: TransactionDigest,
        deleted: BTreeMap<ObjectID, (SequenceNumber, DeleteKind)>,
        written: BTreeMap<ObjectID, (ObjectRef, Object, WriteKind)>,
    },

    // Execution Worker <-> Storage Engine
    StateUpdate(TransactionEffects),
    Checkpointed(u64),

    // For connection setup
    Handshake(),
}

impl Message for SailfishMessage {
    fn serialize(&self) -> String {
        serde_json::to_string(self).unwrap()
    }

    fn deserialize(string: String) -> Self {
        serde_json::from_str(&string).unwrap()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionWithEffects {
    pub tx: SenderSignedData,
    pub ground_truth_effects: Option<TransactionEffects>, // full effects of tx, as ground truth exec result
    pub child_inputs: Option<Vec<ObjectID>>,              // TODO: mark mutable
    pub checkpoint_seq: Option<u64>,
}

impl TransactionWithEffects {
    pub fn is_epoch_change(&self) -> bool {
        match self.tx.transaction_data().kind() {
            TransactionKind::ChangeEpoch(_) => true,
            _ => false,
        }
    }

    /// Returns the read set of a transction.
    /// Specifically, this is the set of input objects to the transaction.
    /// It excludes child objects that are determined at runtime,
    /// but includes all owned objects inputs that must have their version numbers bumped.
    pub fn get_read_set(&self) -> HashSet<ObjectID> {
        let tx_data = self.tx.transaction_data();
        let input_object_kinds = tx_data
            .input_objects()
            .expect("Cannot get input object kinds");

        let mut read_set = HashSet::new();
        for kind in &input_object_kinds {
            match kind {
                InputObjectKind::MovePackage(id)
                | InputObjectKind::SharedMoveObject { id, .. }
                | InputObjectKind::ImmOrOwnedMoveObject((id, _, _)) => read_set.insert(*id),
            };
        }

        for (gas_obj_id, _, _) in tx_data.gas().iter() {
            // skip genesis gas objects
            if *gas_obj_id != ObjectID::from_single_byte(0) {
                read_set.insert(*gas_obj_id);
            }
        }

        for (&package_obj_id, _, _) in tx_data.move_calls() {
            read_set.insert(package_obj_id);
        }

        return read_set;
    }

    /// TODO: This makes use of ground_truth_effects, which is illegal for validators;
    /// it is not something that is known a-priori before execution.
    /// Returns the write set of a transction.
    pub fn get_write_set(&self) -> HashSet<ObjectID> {
        match &self.ground_truth_effects {
            Some(fx) => {
                let TransactionEffects::V1(tx_effects) = fx;
                let total_writes = tx_effects.created.len()
                    + tx_effects.mutated.len()
                    + tx_effects.unwrapped.len()
                    + tx_effects.deleted.len()
                    + tx_effects.unwrapped_then_deleted.len()
                    + tx_effects.wrapped.len();
                let mut write_set: HashSet<ObjectID> = HashSet::with_capacity(total_writes);

                write_set.extend(
                    tx_effects
                        .created
                        .iter()
                        .chain(tx_effects.mutated.iter())
                        .chain(tx_effects.unwrapped.iter())
                        .map(|(object_ref, _)| object_ref.0),
                );
                write_set.extend(
                    tx_effects
                        .deleted
                        .iter()
                        .chain(tx_effects.unwrapped_then_deleted.iter())
                        .chain(tx_effects.wrapped.iter())
                        .map(|object_ref| object_ref.0),
                );

                write_set
            }
            None => self.get_read_set(),
        }
        // assert!(self.ground_truth_effects.is_some());
    }

    /// Returns the read-write set of the transaction.
    pub fn get_read_write_set(&self) -> HashSet<ObjectID> {
        self.get_read_set()
            .union(&self.get_write_set())
            .copied()
            .collect()
    }

    pub fn get_relevant_ews(&self, num_ews: u8) -> HashSet<u8> {
        let rw_set = self.get_read_write_set();
        if rw_set.contains(&ObjectID::from_single_byte(5)) || self.is_epoch_change() {
            (0..num_ews).collect()
        } else {
            rw_set
                .into_iter()
                .map(|obj_id| obj_id[0] % num_ews)
                .collect()
        }
    }
}

pub struct TransactionWithResults {
    pub full_tx: TransactionWithEffects,
    pub tx_effects: TransactionEffects, // determined after execution
    pub deleted: BTreeMap<ObjectID, (SequenceNumber, DeleteKind)>,
    pub written: BTreeMap<ObjectID, (ObjectRef, Object, WriteKind)>,
    pub missing_objs: HashSet<ObjectID>,
}

#[derive(PartialEq)]
pub enum ExecutionMode {
    Channel,
    Database,
}
