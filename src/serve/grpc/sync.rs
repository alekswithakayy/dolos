use futures_core::Stream;
use pallas::{
    crypto::hash::Hash,
    ledger::traverse::{Era, MultiEraBlock, MultiEraOutput, OriginalHash},
    storage::rolldb::{chain, wal},
};
use std::collections::HashMap;
use std::pin::Pin;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};
use utxorpc_spec::utxorpc::v1alpha as u5c;

use crate::storage::applydb::ApplyDB;

fn bytes_to_hash(raw: &[u8]) -> Hash<32> {
    let array: [u8; 32] = raw.try_into().unwrap();
    Hash::<32>::new(array)
}

fn fetch_stxi(hash: Hash<32>, idx: u64, ledger: &ApplyDB) -> u5c::cardano::TxOutput {
    let (era, cbor) = ledger.get_stxi(hash, idx).unwrap().unwrap();
    let era = Era::try_from(era).unwrap();
    let txo = MultiEraOutput::decode(era, &cbor).unwrap();
    pallas::interop::utxorpc::map_tx_output(&txo)
}

fn raw_to_anychain(raw: &[u8], ledger: &ApplyDB) -> u5c::sync::AnyChainBlock {
    let block = MultiEraBlock::decode(raw).unwrap();

    let mut datum_map = HashMap::new();
    for tx in block.txs().into_iter() {
        for plutus_datum in tx.plutus_data().iter() {
            let hash = plutus_datum.original_hash();
            datum_map.insert(hash, plutus_datum.clone());
        }
    }

    let mut block = pallas::interop::utxorpc::map_block(&block);

    let input_refs: Vec<_> = block
        .body
        .iter()
        .flat_map(|b| b.tx.iter())
        .flat_map(|t| t.inputs.iter())
        .map(|i| ((bytes_to_hash(&i.tx_hash), i.output_index), i))
        .collect();

    let stxis: HashMap<_, _> = input_refs
        .iter()
        .map(|&(ref key, _)| {
            let (hash, idx) = key;
            let stxi = fetch_stxi(*hash, *idx as u64, &ledger);
            (*key, stxi)
        })
        .collect();

    for tx in block.body.as_mut().unwrap().tx.iter_mut() {
        for input in tx.inputs.iter_mut() {
            let key = (bytes_to_hash(&input.tx_hash), input.output_index);
            match stxis.get(&key) {
                Some(output) => {
                    let mut as_output = output.clone();

                    if output.datum_hash.len() == 32 {
                        let datum_hash = Hash::<32>::from(&*output.datum_hash);

                        if let Some(datum_value) = datum_map.get(&datum_hash) {
                            as_output.datum =
                                Some(pallas::interop::utxorpc::map_plutus_datum(&datum_value));
                        }
                    }

                    input.as_output = Some(as_output);
                }
                None => panic!(
                    "STXI not found for hash: {}, index: {}",
                    bytes_to_hash(&input.tx_hash).to_string(),
                    input.output_index
                ),
            }
        }
    }

    u5c::sync::AnyChainBlock {
        chain: u5c::sync::any_chain_block::Chain::Cardano(block).into(),
    }
}

fn roll_to_tip_response(log: wal::Log, ledger: &ApplyDB) -> u5c::sync::FollowTipResponse {
    u5c::sync::FollowTipResponse {
        action: match log {
            wal::Log::Apply(_, _, block) => {
                u5c::sync::follow_tip_response::Action::Apply(raw_to_anychain(&block, ledger))
                    .into()
            }
            wal::Log::Undo(_, _, block) => {
                u5c::sync::follow_tip_response::Action::Undo(raw_to_anychain(&block, ledger)).into()
            }
            // TODO: shouldn't we have a u5c event for origin?
            wal::Log::Origin => None,
            wal::Log::Mark(..) => None,
        },
    }
}

pub struct ChainSyncServiceImpl {
    wal: wal::Store,
    chain: chain::Store,
    ledger: ApplyDB,
}

impl ChainSyncServiceImpl {
    pub fn new(wal: wal::Store, chain: chain::Store, ledger: ApplyDB) -> Self {
        Self { wal, chain, ledger }
    }
}

#[async_trait::async_trait]
impl u5c::sync::chain_sync_service_server::ChainSyncService for ChainSyncServiceImpl {
    type FollowTipStream =
        Pin<Box<dyn Stream<Item = Result<u5c::sync::FollowTipResponse, Status>> + Send + 'static>>;

    async fn fetch_block(
        &self,
        request: Request<u5c::sync::FetchBlockRequest>,
    ) -> Result<Response<u5c::sync::FetchBlockResponse>, Status> {
        let message = request.into_inner();

        let blocks: Result<Vec<_>, _> = message
            .r#ref
            .iter()
            .map(|r| bytes_to_hash(&r.hash))
            .map(|hash| self.chain.get_block(hash))
            .collect();

        let out: Vec<_> = blocks
            .map_err(|_err| Status::internal("can't query block"))?
            .iter()
            .flatten()
            .map(|b| raw_to_anychain(b, &self.ledger))
            .collect();

        let response = u5c::sync::FetchBlockResponse { block: out };

        Ok(Response::new(response))
    }

    async fn dump_history(
        &self,
        request: Request<u5c::sync::DumpHistoryRequest>,
    ) -> Result<Response<u5c::sync::DumpHistoryResponse>, Status> {
        let msg = request.into_inner();
        let from = msg.start_token.map(|r| r.index).unwrap_or_default();
        let len = msg.max_items as usize + 1;

        let mut page: Vec<_> = self
            .chain
            .read_chain_page(from, len)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_err| Status::internal("can't query history"))?;

        let next_token = if page.len() == len {
            let (next_slot, next_hash) = page.remove(len - 1);
            Some(u5c::sync::BlockRef {
                index: next_slot,
                hash: next_hash.to_vec().into(),
            })
        } else {
            None
        };

        let blocks = page
            .into_iter()
            .map(|(_, hash)| self.chain.get_block(hash))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_err| Status::internal("can't query history"))?
            .into_iter()
            .map(|x| x.ok_or(Status::internal("can't query history")))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|raw| raw_to_anychain(&raw, &self.ledger))
            .collect();

        let response = u5c::sync::DumpHistoryResponse {
            block: blocks,
            next_token,
        };

        Ok(Response::new(response))
    }

    async fn follow_tip(
        &self,
        request: Request<u5c::sync::FollowTipRequest>,
    ) -> Result<Response<Self::FollowTipStream>, tonic::Status> {
        let request = request.into_inner();

        let intersect: Vec<_> = request
            .intersect
            .iter()
            .map(|x| (x.index, bytes_to_hash(&x.hash)))
            .collect();

        let ledger = self.ledger.clone();

        let s = wal::RollStream::intersect(self.wal.clone(), intersect)
            .map(move |log| Ok(roll_to_tip_response(log, &ledger)));

        Ok(Response::new(Box::pin(s)))
    }
}
