/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "tipset_loader.hpp"

#include <cassert>

#include "primitives/cid/cid_of_cbor.hpp"

namespace fc::sync {
  namespace {
    auto log() {
      static common::Logger logger = common::createLogger("sync");
      return logger.get();
    }

    inline bool needToSync(storage::indexdb::SyncState s) {
      return s == storage::indexdb::SYNC_STATE_UNKNOWN
             || s == storage::indexdb::SYNC_STATE_UNSYNCED;
    }

    outcome::result<BlockHeader> extractBlockHeader(
        storage::ipfs::IpfsDatastore &db, const CID &cid) {
      auto res = db.getCbor<BlockHeader>(cid);
      if (!res) {
        log()->error(
            "cannot load BlockHeader from local storage, cid={}, error={}",
            cid.toString(),
            res.error().message());
      }
      return res;
    }

    outcome::result<void> appendBlockHeader(
        storage::ipfs::IpfsDatastore &db,
        const CID &cid,
        std::vector<std::pair<CID, BlockHeader>> &blocks) {
      auto bh = extractBlockHeader(db, cid);
      if (bh) {
        blocks.push_back({cid, std::move(bh.value())});
        return outcome::success();
      }
      return Error::SYNC_DATA_INTEGRITY_ERROR;
    }

    boost::optional<BlockMsg> tryDecodeBlockMsg(gsl::span<const uint8_t> data) {
      if (data.empty() || data[0] != 0x83) {
        return boost::none;
      }
      auto res = codec::cbor::decode<BlockMsg>(data);
      if (!res) {
        return boost::none;
      }
      return res.value();
    }

  }  // namespace

  TipsetLoader::RequestCtx::RequestCtx(TipsetLoader &o, const TipsetKey &key)
      : owner(o), tipset_key(key) {}

  void TipsetLoader::RequestCtx::onBlockSynced(const CID &cid,
                                               const BlockHeader &bh) {
    if (is_bad_tipset) {
      return;
    }

    auto it = wantlist.find(cid);
    if (it == wantlist.end()) {
      // not our block
      return;
    }

    wantlist.erase(it);

    const auto &cids = tipset_key.cids();
    size_t pos = 0;
    for (; pos < cids.size(); ++pos) {
      if (cids[pos] == cid) break;
    }

    assert(pos <= cids.size());

    blocks_filled[pos] = bh;

    if (!wantlist.empty()) {
      // wait for remaining blocks
      return;
    }

    call_completed = owner.scheduler_->schedule([this]() {
      auto res = Tipset::create(tipset_key, std::move(blocks_filled));
      owner.onRequestCompleted(std::move(res), tipset_key);
    });
  }

  void TipsetLoader::RequestCtx::onCidIsNotABlock(const CID &cid) {
    if (is_bad_tipset) {
      return;
    }

    auto it = wantlist.find(cid);
    if (it == wantlist.end()) {
      // not our block
      return;
    }

    is_bad_tipset = true;

    call_completed = owner.scheduler_->schedule([this]() {
      owner.onRequestCompleted(Error::SYNC_BAD_TIPSET, tipset_key);
    });
  }

  void TipsetLoader::onRequestCompleted(outcome::result<Tipset> tipset,
                                        const TipsetKey &tipset_key) {
    callback_(tipset_key, std::move(tipset));
    tipset_requests_.erase(tipset_key.hash());
  }

  outcome::result<boost::optional<Tipset>> TipsetLoader::loadTipset(
      const TipsetKey &key,
      boost::optional<std::reference_wrapper<const PeerId>> preferred_peer) {
    using namespace storage::indexdb;

    if (tipset_requests_.count(key.hash()) != 0) {
      // already waiting, do nothing
      return boost::none;
    }

    OUTCOME_TRY(sync_state, index_db_->getTipsetSyncState(key.hash()));

    if (sync_state == SYNC_STATE_BAD) {
      return Error::SYNC_BAD_TIPSET;
    }

    OUTCOME_TRY(blocks_available,
                block_loader_->loadBlocks(key, preferred_peer));

    size_t n = key.cids().size();

    assert(blocks_available.size() == key.cids().size());

    Wantlist wantlist;

    for (size_t i = 0; i < n; ++i) {
      if (!blocks_available[i].has_value()) {
        wantlist.insert(key.cids()[i]);
      }
    }

    if (wantlist.empty()) {
      auto res = Tipset::create(key, std::move(blocks_available));
      if (!res) {
        log()->error("TipsetLoader: cannot create tipset, err=",
                     res.error().message());
        return Error::SYNC_BAD_TIPSET;
      }
      return res.value();
    }

    RequestCtx ctx(*this, key);
    ctx.wantlist = std::move(wantlist);
    ctx.blocks_filled = std::move(blocks_available);

    tipset_requests_.insert({key.hash(), std::move(ctx)});
    return boost::none;
  }

}  // namespace fc::sync
