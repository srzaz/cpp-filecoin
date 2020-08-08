/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <libp2p/injector/gossip_injector.hpp>
#include <libp2p/protocol/identify/identify.hpp>
#include <libp2p/protocol/identify/identify_delta.hpp>
#include <libp2p/protocol/identify/identify_push.hpp>

#include "api/make.hpp"
#include "api/rpc/ws.hpp"
#include "blockchain/impl/weight_calculator_impl.hpp"
#include "common/span.hpp"
#include "crypto/bls/impl/bls_provider_impl.hpp"
#include "crypto/secp256k1/impl/secp256k1_provider_impl.hpp"
#include "node/blocksync.hpp"
#include "node/hello.hpp"
#include "node/peermgr.hpp"
#include "node/sync.hpp"
#include "storage/car/car.hpp"
#include "storage/chain/impl/chain_store_impl.hpp"
#include "storage/in_memory/in_memory_storage.hpp"
#include "storage/ipfs/impl/in_memory_datastore.hpp"
#include "storage/keystore/impl/in_memory/in_memory_keystore.hpp"
#include "vm/actor/builtin/init/init_actor.hpp"
#include "vm/interpreter/impl/interpreter_impl.hpp"
#include "vm/state/impl/state_tree_impl.hpp"

namespace fc {
  using api::BlockWithCids;
  using api::SignedMessage;
  using api::Tipset;
  using blockchain::weight::WeightCalculatorImpl;
  using boost::asio::io_context;
  using libp2p::peer::PeerId;
  using storage::blockchain::ChainStoreImpl;
  using sync::Sync;
  using sync::TsSync;
  using vm::interpreter::InterpreterImpl;

  boost::optional<Buffer> readFile(std::string_view path) {
    std::ifstream file{path, std::ios::binary | std::ios::ate};
    if (!file.good()) {
      return boost::none;
    }
    Buffer buffer;
    buffer.resize(file.tellg());
    file.seekg(0, std::ios::beg);
    file.read(common::span::string(buffer).data(), buffer.size());
    return buffer;
  }

  struct Config {
    int p2p_port, api_port;
    std::string genesis_path;
    std::string network_name;
    CID genesis_cid;
  };

  outcome::result<std::string> readGenesis(IpldPtr ipld, Config &config) {
    auto car{readFile(config.genesis_path)};
    assert(car);  // TODO
    OUTCOME_TRY(roots, storage::car::loadCar(*ipld, *car));
    config.genesis_cid = roots[0];
    OUTCOME_TRY(ts, Tipset::load(*ipld, {config.genesis_cid}));
    assert(ts.height == 0);  // TODO
    OUTCOME_TRY(init_state,
                vm::state::StateTreeImpl{ipld, ts.getParentStateRoot()}
                    .state<vm::actor::builtin::init::InitActorState>(
                        vm::actor::kInitAddress));
    OUTCOME_TRY(InterpreterImpl{}.interpret(ipld, ts));
    OUTCOME_TRY(WeightCalculatorImpl{ipld}.calculateWeight(ts));
    config.network_name = init_state.network_name;
    return outcome::success();
  }

  outcome::result<void> main() {
    auto numenv{[](auto &name, auto def) {
      if (auto str{getenv(name)}) {
        return std::stoi(str);
      }
      return def;
    }};
    Config config;
    config.p2p_port = numenv("FH_P2P_PORT", 3020);
    config.api_port = numenv("FH_API_PORT", 3021);
    config.genesis_path = getenv("FH_GENESIS");
    assert(!config.genesis_path.empty());
    auto ipld{std::make_shared<storage::ipfs::InMemoryDatastore>()};
    OUTCOME_TRY(readGenesis(ipld, config));
    auto weight_calculator{std::make_shared<WeightCalculatorImpl>(ipld)};
    OUTCOME_TRY(genesis, Tipset::load(*ipld, {config.genesis_cid}));
    auto chain_store{std::make_shared<ChainStoreImpl>(
        ipld, weight_calculator, genesis.blks[0], genesis)};
    auto injector{libp2p::injector::makeGossipInjector()};
    auto io{injector.create<std::shared_ptr<io_context>>()};
    auto host{injector.create<std::shared_ptr<libp2p::Host>>()};
    auto mpool{storage::mpool::Mpool::create(ipld, chain_store)};
    auto interpreter{std::make_shared<vm::interpreter::CachedInterpreter>(
        std::make_shared<InterpreterImpl>(),
        std::make_shared<storage::InMemoryStorage>())};
    auto msg_waiter{storage::blockchain::MsgWaiter::create(ipld, chain_store)};
    auto keystore{std::make_shared<storage::keystore::InMemoryKeyStore>(
        std::make_shared<crypto::bls::BlsProviderImpl>(),
        std::make_shared<crypto::secp256k1::Secp256k1ProviderImpl>())};
    auto sync{std::make_shared<Sync>(
        ipld, std::make_shared<TsSync>(host, ipld, interpreter), chain_store)};
    auto hello{std::make_shared<hello::Hello>(
        host, chain_store, [&](auto &peer, auto state) {
          spdlog::info("hello from {}: height={}, weight={}, blocks={}",
                       peer.toBase58(),
                       state.height,
                       state.weight,
                       fmt::join(state.blocks, ","));
          sync->onHello(state.blocks, peer);
        })};
    auto peermgr{std::make_shared<peermgr::PeerMgr>(
        host,
        injector.create<std::shared_ptr<libp2p::protocol::Identify>>(),
        injector.create<std::shared_ptr<libp2p::protocol::IdentifyPush>>(),
        injector.create<std::shared_ptr<libp2p::protocol::IdentifyDelta>>(),
        hello)};
    blocksync::serve(host, ipld);
    auto gossip{
        injector.create<std::shared_ptr<libp2p::protocol::gossip::Gossip>>()};
    auto block_sub{gossip->subscribe(
        {fmt::format("/fil/blocks/{}", config.network_name)},
        [&](auto message) {
          if (message) {
            if (auto peer{PeerId::fromBytes(message->from)}) {
              if (auto block{
                      codec::cbor::decode<BlockWithCids>(message->data)}) {
                std::ignore = sync->onGossip(block.value(), peer.value());
              }
            }
          }
        })};
    auto message_sub{gossip->subscribe(
        {fmt::format("/fil/msgs/{}", config.network_name)}, [&](auto message) {
          if (message) {
            if (auto peer{PeerId::fromBytes(message->from)}) {
              if (auto signed_message{
                      codec::cbor::decode<SignedMessage>(message->data)}) {
                std::ignore = mpool->add(signed_message.value());
              }
            }
          }
        })};
    gossip->start();
    OUTCOME_TRY(listening,
                libp2p::multi::Multiaddress::create(
                    fmt::format("/ip4/127.0.0.1/tcp/{}", config.p2p_port)));
    auto _api{api::makeImpl(chain_store,
                            weight_calculator,
                            ipld,
                            mpool,
                            interpreter,
                            msg_waiter,
                            // TODO: expired certificates, deploy own drand
                            nullptr,
                            keystore)};
    _api.NetAddrsListen = {[&]() {
      return libp2p::peer::PeerInfo{host->getId(), {listening}};
    }};
    api::serve(_api, *io, "127.0.0.1", config.api_port);
    OUTCOME_TRY(host->listen(listening));
    host->start();
    spdlog::info("started with genesis {}", chain_store->genesisCid());
    io->run();
    return outcome::success();
  }
}  // namespace fc

int main(int argc, char **argv) {
  OUTCOME_EXCEPT(fc::main());
}
