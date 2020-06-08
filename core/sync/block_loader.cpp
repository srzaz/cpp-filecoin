/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "block_loader.hpp"

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

  //  Downloader::BlockCrafter::BlockCrafter(Downloader &o, CID c)
  //      : owner(o), block_cid(std::move(c)) {}

  outcome::result<BlockLoader::BlocksAvailable> BlockLoader::loadBlocks(
      const std::vector<CID> &cids,
      boost::optional<std::reference_wrapper<const PeerId>> preferred_peer) {
    using namespace storage::indexdb;

    if (!initialized_) {
      return Error::SYNC_NOT_INITIALIZED;
    }

    BlocksAvailable blocks_available;
    if (cids.empty()) {
      return blocks_available;
    }
    blocks_available.resize(cids.size());

    Wantlist headers_to_load;
    Wantlist meta_to_load;
    Wantlist bls_messages_to_load;
    Wantlist secp_messages_to_load;

    size_t idx = 0;
    for (const auto &cid : cids) {
      OUTCOME_TRY(block_available,
                  findBlock(cid, bls_messages_to_load, secp_messages_to_load));

      if (block_available.current_state == SYNC_STATE_SYNCED) {
        // all subobjects are in local storage
        blocks_available[idx] = std::move(block_available.header);
      }

      ++idx;
    }

    return blocks_available;
  }

  outcome::result<BlockLoader::BlockAvailable> BlockLoader::findBlock(
      const CID &cid,
      Wantlist &bls_messages_to_load,
      Wantlist &secp_messages_to_load) {
    using namespace storage::indexdb;

    OUTCOME_TRY(info, index_db_->getObjectInfo(cid));

    if (info.type != OBJECT_TYPE_UNKNOWN && info.type != OBJECT_TYPE_BLOCK) {
      return Error::SYNC_BAD_BLOCK;
    }

    if (info.sync_state < SYNC_STATE_HEADER_SYNCED) {
      return BlockAvailable{info.sync_state, boost::none};
    }

    auto header_res = ipfs_db_->getCbor<BlockHeader>(cid);
    if (!header_res) {
      log()->error("no block header in storage, cid={}, state={}",
                   cid.toString(),
                   header_res.error().message());
      return Error::SYNC_DATA_INTEGRITY_ERROR;
    }

    if (info.sync_state != SYNC_STATE_SYNCED && info.msg_cid.has_value()) {
      // header is synced, but messages are not
      assert(info.sync_state == SYNC_STATE_HEADER_SYNCED);

      if (info.msg_cid.value() != header_res.value().messages) {
        log()->error("messages cid doesn't match for block cid {}, ({} != {})",
                     cid.toString(),
                     info.msg_cid.value().toString(),
                     header_res.value().messages.toString());
        return Error::SYNC_DATA_INTEGRITY_ERROR;
      }

      OUTCOME_TRY(index_db_->getUnsyncedMessagesOfBlock(
          cid, [&](const CID &message_cid, ObjectType type) {
            if (type == OBJECT_TYPE_BLS_MESSAGE) {
              bls_messages_to_load.insert(message_cid);
            } else if (type == OBJECT_TYPE_SECP_MESSAGE) {
              secp_messages_to_load.insert(message_cid);
            } else {
              log()->error("unknown message type {}, cid={}",
                           type,
                           message_cid.toString());
            }
          }));
    }

    return BlockAvailable{info.sync_state, std::move(header_res.value())};
  }

  /*
    void BlockLoader::RequestCtx::onHeaderSynced(const BlockMsg &msg) {
      header_wanted = false;
      block_msg = msg;
      msgs_wantlist.insert(msg.bls_messages.begin(), msg.bls_messages.end());
      msgs_wantlist.insert(msg.secp_messages.begin(), msg.secp_messages.end());

      if (msgs_wantlist.empty()) {
        sayCompleted();
      }
    }

    void BlockLoader::RequestCtx::onMessageSynced(const CID &msg_cid) {
      if (header_wanted) {
        // header first
        return;
      }
      msgs_wantlist.erase(msg_cid);
      if (msgs_wantlist.empty()) {
        sayCompleted();
      }
    }

    void BlockLoader::RequestCtx::sayCompleted() {
      if (!block_msg) {
        // should never happen
        return;
      }
      call_completed_ = owner.scheduler_->schedule(
          [this]() { owner.onBlockCompleted(block_cid, block_msg.value()); });
    }

    void BlockLoader::onBlockCompleted(const CID &block_cid, const BlockMsg
    &msg) { static const common::Buffer empty_buffer{};

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


    outcome::result<void> BlockLoader::loadBlock(
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

    outcome::result<void> BlockLoader::loadMessages(const std::vector<CID>
    &cids) {
      // TODO use batch protocol like blocksync
      for (const auto &cid : cids) {
        OUTCOME_TRY(loadObjectByCid(cid, boost::none, message_expected));
      }
      return outcome::success();
    }

  */

}  // namespace fc::sync
