use std::{collections::HashMap, sync::Arc};

use gasket::framework::*;
use pallas::crypto::hash::Hash;
use tokio::sync::RwLock;
use tracing::debug;

use super::{monitor::BlockMonitorMessage, BlockHeight, BlockSlot, Transaction};

pub type SubmitEndpointReceiver = gasket::messaging::InputPort<Vec<Transaction>>;
pub type BlockMonitorReceiver = gasket::messaging::InputPort<BlockMonitorMessage>;

pub type PropagatorSender = gasket::messaging::OutputPort<Vec<Transaction>>;

type InclusionPoint = BlockHeight;

#[derive(Debug)]
pub enum MempoolEvent {
    AddTxs(Vec<Transaction>),
    ChainUpdate(BlockMonitorMessage),
}

#[derive(Default)]
pub struct MempoolState(pub RwLock<Monitor>, pub tokio::sync::Notify);

#[derive(Default)]
pub struct Monitor {
    pub tip_slot: BlockSlot,
    pub txs: HashMap<Hash<32>, Option<InclusionPoint>>,
}

#[derive(Stage)]
#[stage(name = "mempool", unit = "MempoolEvent", worker = "Worker")]
pub struct Stage {
    pub state: Arc<MempoolState>,

    pub prune_height: u64,
    // TODO: prune txs even if they never land on chain?
    pub upstream_submit_endpoint: SubmitEndpointReceiver,
    pub upstream_block_monitor: BlockMonitorReceiver,
    pub downstream_propagator: PropagatorSender,
    // #[metric]
    // received_txs: gasket::metrics::Counter,
}

impl Stage {
    pub fn new(state: Arc<MempoolState>, prune_height: u64) -> Self {
        Self {
            state,
            prune_height,
            upstream_submit_endpoint: Default::default(),
            upstream_block_monitor: Default::default(),
            downstream_propagator: Default::default(),
        }
    }
}

pub struct Worker {}

impl Worker {}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(_stage: &Stage) -> Result<Self, WorkerError> {
        Ok(Self {})
    }

    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<MempoolEvent>, WorkerError> {
        tokio::select! {
            txs_msg = stage.upstream_submit_endpoint.recv() => {
                let txs_msg = txs_msg.or_panic()?;
                debug!("received txs message: {:?}", txs_msg);
                Ok(WorkSchedule::Unit(MempoolEvent::AddTxs(txs_msg.payload)))
            }
            monitor_msg = stage.upstream_block_monitor.recv() => {
                let monitor_msg = monitor_msg.or_panic()?;
                debug!("received monitor message: {:?}", monitor_msg);
                Ok(WorkSchedule::Unit(MempoolEvent::ChainUpdate(monitor_msg.payload)))
            }
        }
    }

    async fn execute(&mut self, unit: &MempoolEvent, stage: &mut Stage) -> Result<(), WorkerError> {
        match unit {
            MempoolEvent::AddTxs(txs) => {
                let mut txs = txs.clone();

                // pass new txs to downstream/propagate txs
                stage
                    .downstream_propagator
                    .send(txs.clone().into())
                    .await
                    .or_panic()?;

                let mut monitor = stage.state.0.write().await;

                // do not overwrite in the tx monitor map
                txs.retain(|x| !monitor.txs.contains_key(&x.hash));

                // make note of new txs for monitoring
                monitor
                    .txs
                    .extend(txs.clone().into_iter().map(|x| (x.hash, None)));
            }
            MempoolEvent::ChainUpdate(monitor_msg) => {
                match monitor_msg {
                    BlockMonitorMessage::NewBlock(slot, block_txs) => {
                        let mut monitor = stage.state.0.write().await;

                        // set inclusion point for txs found in new block
                        for (tx_hash, inclusion) in monitor.txs.iter_mut() {
                            if block_txs.contains(tx_hash) {
                                debug!("setting inclusion point for {}: {slot}", tx_hash);
                                *inclusion = Some(*slot)
                            }
                        }

                        // prune txs which have sufficient confirmations
                        monitor.txs.retain(|_, inclusion| {
                            if let Some(inclusion_slot) = inclusion {
                                slot - *inclusion_slot <= stage.prune_height
                            } else {
                                true
                            }
                        });

                        monitor.tip_slot = *slot;
                    }
                    BlockMonitorMessage::Rollback(rb_slot) => {
                        let mut monitor = stage.state.0.write().await;

                        // remove inclusion points later than rollback slot
                        for (tx_hash, inclusion) in monitor.txs.iter_mut() {
                            if let Some(slot) = inclusion {
                                if *slot > *rb_slot {
                                    debug!(
                                        "removing inclusion point for {} due to rollback ({} > {})",
                                        tx_hash, slot, rb_slot
                                    );

                                    *inclusion = None
                                }
                            }
                        }

                        monitor.tip_slot = *rb_slot;
                    }
                }

                stage.state.1.notify_waiters()
            }
        }

        Ok(())
    }
}
