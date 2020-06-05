/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "downloader.hpp"

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

  Downloader::TipsetCrafter::TipsetCrafter(Downloader &o,
                                           const TipsetKey& key)
      : owner(o), tipset_key(key) {
    const auto& cids = tipset_key.cids();
    wantlist.insert(cids.begin(), cids.end());
    blocks_filled.resize(cids.size());
  }

  void Downloader::TipsetCrafter::onBlockSynced(const CID &cid,
                                                const BlockHeader &bh) {
    auto it = wantlist.find(cid);
    if (it == wantlist.end()) {
      // not our block
      return;
    }

    wantlist.erase(it);

    const auto& cids = tipset_key.cids();
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

    call_completed_ = owner.scheduler_->schedule([this]() {
      std::vector<BlockHeader> blocks;
      blocks.reserve(tipset_key.cids().size());
      for (auto &b : blocks_filled) {
        assert(b.has_value());
        blocks.emplace_back(std::move(b.value()));
      }

      auto res = Tipset::create(std::move(blocks));
      owner.onTipsetCompleted(res, tipset_key);
    });
  }

  Downloader::BlockCrafter::BlockCrafter(Downloader &o, CID c)
      : owner(o), block_cid(std::move(c)) {}

  void Downloader::BlockCrafter::onHeaderSynced(const BlockMsg &msg) {
    header_wanted = false;
    block_msg = msg;
    msgs_wantlist.insert(msg.bls_messages.begin(), msg.bls_messages.end());
    msgs_wantlist.insert(msg.secp_messages.begin(), msg.secp_messages.end());

    if (msgs_wantlist.empty()) {
      sayCompleted();
    }
  }

  void Downloader::BlockCrafter::onMessageSynced(const CID &msg_cid) {
    if (header_wanted) {
      // header first
      return;
    }
    msgs_wantlist.erase(msg_cid);
    if (msgs_wantlist.empty()) {
      sayCompleted();
    }
  }

  void Downloader::BlockCrafter::sayCompleted() {
    if (!block_msg) {
      // should never happen
      return;
    }
    call_completed_ = owner.scheduler_->schedule(
        [this]() { owner.onBlockCompleted(block_cid, block_msg.value()); });
  }

  void Downloader::onBlockCompleted(const CID &block_cid, const BlockMsg &msg) {
    static const common::Buffer empty_buffer{};

    if (!blocks_signal_.empty()) {
      blocks_signal_(FullBlockAvailable{{block_cid,
                                         empty_buffer,
                                         // TODO last_received_from_,
                                         source_from_,
                                         load_success},
                                        msg.header});
    }

    for (auto &tr : tipset_requests_) {
      tr.second.onBlockSynced(block_cid, msg.header);
    }

    block_requests_.erase(block_cid);
  }

  void Downloader::onTipsetCompleted(outcome::result<Tipset> tipset,
                                     const TipsetKey& tipset_key) {
    bool success = tipset.has_value();

    if (success) {
      // TODO move to syncer
//      auto res =
//          storage::indexdb::addTipset(*index_db_,
//                                      tipset.value(),
//                                      hash,
//                                      storage::indexdb::TIPSET_STATE_SYNCED,
//                                      branch_id);
//      if (!res) {
//        // error is already logged inside indexdb
//        success = false;
//      }
    }

//    if (!tipsets_signal_.empty()) {
//      if (success) {
//        tipsets_signal_(TipsetAvailable{
//            load_success, cids, hash, branch_id, tipset.value()});
//      } else {
//        tipsets_signal_(TipsetAvailable{
//            object_is_invalid, cids, hash, branch_id, boost::none});
//      }
//    }

    tipset_requests_.erase(tipset_key.hash());
  }

  outcome::result<void> Downloader::loadTipset(const TipsetKey &key) {
    using namespace storage::indexdb;

    if (tipset_requests_.count(key.hash()) != 0) {
      // already waiting, do nothing
      return outcome::success();
    }

    OUTCOME_TRY(sync_state, index_db_->getTipsetSyncState(key.hash()));

    if (!needToSync(sync_state)) {
      return outcome::success();
    }

    std::vector<std::pair<CID, BlockHeader>> blocks;

    for (const auto& cid : key.cids()) {
      OUTCOME_TRY(info, index_db_->getObjectInfo(cid));

      if (info.type == OBJECT_TYPE_UNKNOWN) {

        OUTCOME_TRY(loadBlock(cid, SYNC_STATE_UNKNOWN));

      } else if (info.type != OBJECT_TYPE_BLOCK) {

        log()->error("loadTipset: {} is not a block", cid.toString());
        return Error::SYNC_WRONG_OBJECT_TYPE;

      } else if (info.sync_state == SYNC_STATE_SYNCED) {

        OUTCOME_TRY(appendBlockHeader(*ipfs_db_, cid, blocks));

      } else {
        OUTCOME_TRY(loadBlock(cid, info.sync_state));
      }
    }

    auto p = tipset_requests_.insert(
        {key.hash(), TipsetCrafter(*this, key)});

    assert(p.second);

    TipsetCrafter &crafter = p.first->second;

    for (const auto &[cid, bh] : blocks) {
      // feeding existing blocks
      crafter.onBlockSynced(cid, bh);
    }

    return outcome::success();
  }

  outcome::result<void> Downloader::loadBlock(
      const CID &cid, storage::indexdb::SyncState current_state) {
    using namespace storage::indexdb;

    if (block_requests_.count(cid) != 0) {
      // already waiting, do nothing
      return outcome::success();
    }

    boost::optional<BlockMsg> header_we_got;
    std::vector<CID> messages_we_got;
    std::vector<CID> messages_to_load;

    if (current_state == SYNC_STATE_HEADER_SYNCED) {
      OUTCOME_TRY(header, extractBlockHeader(*ipfs_db_, cid));
      header_we_got = BlockMsg{std::move(header), {}, {}};

      OUTCOME_TRY(pairs,
                  getMessagesOfBlock(*index_db_, cid, header_we_got.value()));

      for (const auto &[cid, state] : pairs) {
        switch (state) {
          case NOT_A_MESSAGE:
            log()->error("loadBlock: {} is not a message", cid.toString());
            return Error::SYNC_WRONG_OBJECT_TYPE;

          case MESSAGE_STATE_UNKNOWN:
            messages_to_load.push_back(cid);
            break;

          case MESSAGE_STATE_SYNCED:
            messages_we_got.push_back(cid);
            break;

          default:
            log()->critical(
                "loadBlock: integrity error, unknown "
                "state {} for cid {}",
                state,
                cid.toString());
            return Error::SYNC_DATA_INTEGRITY_ERROR;
        }
      }

    } else if (current_state != SYNC_STATE_UNKNOWN) {
      log()->error("loadBlock: unacceptable state {} of {}",
                   current_state,
                   cid.toString());
      return Error::SYNC_UNEXPECTED_OBJECT_STATE;
    }

    auto p = block_requests_.insert({cid, BlockCrafter(*this, cid)});

    assert(p.second);

    if (header_we_got) {
      OUTCOME_TRY(loadMessages(messages_to_load));

      BlockCrafter &crafter = p.first->second;
      crafter.onHeaderSynced(header_we_got.value());
      for (const auto &cid_message : messages_we_got) {
        crafter.onMessageSynced(cid_message);
      }

    } else {
      OUTCOME_TRY(loadObjectByCid(cid, boost::none, block_expected));
    }

    return outcome::success();
  }

  outcome::result<void> Downloader::loadMessages(const std::vector<CID> &cids) {
    // TODO use batch protocol like blocksync
    for (const auto &cid : cids) {
      OUTCOME_TRY(loadObjectByCid(cid, boost::none, message_expected));
    }
    return outcome::success();
  }

  outcome::result<void> Downloader::loadObjectByCid(
      CID cid,
      boost::optional<PeerId> from,
      ExpectedObjectType what_to_expect) {
    if (!from && !preferred_peer_) {
      return Error::SYNC_NO_PEERS;
    }

    auto it = cid_requests_.find(cid);
    if (it == cid_requests_.end()) {
      return outcome::success();
    }

    const PeerId &peer = from ? from.value() : preferred_peer_.value();

    static const std::vector<storage::ipfs::graphsync::Extension> extensions;

    auto req = std::make_shared<CIDRequest>();

    req->subscription =
        graphsync_->makeRequest(peer,
                                boost::none,
                                cid,
                                {},
                                extensions,
                                makeRequestProgressCallback(cid));

    req->what_to_expect = what_to_expect;

    cid_requests_.insert(std::pair(cid, std::move(req)));
  }

  storage::ipfs::graphsync::Graphsync::RequestProgressCallback
  Downloader::makeRequestProgressCallback(CID cid) {
    using namespace storage::ipfs::graphsync;

    return [this, cid = std::move(cid)](ResponseStatusCode code,
                                        const std::vector<Extension> &) {
      log()->debug("graphsync request progress for {}: code={}",
                   cid.toString(),
                   statusCodeToString(code));
      onGraphsyncResponseProgress(cid, code);
    };
  }

  void Downloader::onGraphsyncResponseProgress(
      const CID &cid, storage::ipfs::graphsync::ResponseStatusCode status) {
    using namespace storage::ipfs::graphsync;

    if (!isTerminal(status)) {
      log()->debug("response from graphsync for {}: {}, waiting further",
                   cid.toString(),
                   statusCodeToString(status));
      return;
    }

    if (isError(status)) {
      need_to_repeat_request_.insert(cid);
      return;
    }

    // TODO extensions and partial response
  }

  void Downloader::onGraphsyncData(const CID &cid, const common::Buffer &data) {
    boost::optional<BlockMsg> maybe_block = tryDecodeBlockMsg(data);
    boost::optional<UnsignedMessage> maybe_message;
    boost::optional<Signature> maybe_signature;
    bool right_cid = false;

    if (maybe_block) {
      // TODO performance
      auto res = primitives::cid::getCidOfCbor(maybe_block.value().header);
      if (res && res.value() == cid) {
        right_cid = true;
        auto it = block_requests_.find(cid);
        if (it != block_requests_.end()) {
          it->second.onHeaderSynced(maybe_block.value());
        }
      }
      if (!headers_signal_.empty()) {
        headers_signal_(
            BlockHeaderAvailable{{cid,
                                  data,
                                  ObjectAvailable::from_graphsync,
                                  right_cid ? load_success : object_is_invalid},
                                  maybe_block.value()});
      }
    } else {
      auto res = common::getCidOf(data);
      if (res && res.value() == cid) {
        right_cid = true;
      }
    }


  }

}  // namespace fc::sync
