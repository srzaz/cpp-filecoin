/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef CPP_FILECOIN_SYNC_BLOCK_LOADER_HPP
#define CPP_FILECOIN_SYNC_BLOCK_LOADER_HPP

#include "common.hpp"

#include <libp2p/protocol/common/scheduler.hpp>

#include "storage/indexdb/indexdb.hpp"

namespace fc::sync {

  class ObjectLoader {
   public:
    using ObjectsAvailable = std::vector<common::Buffer>;

    outcome::result<ObjectsAvailable> loadObjects(
        const std::vector<CID> &cids,
        boost::optional<std::reference_wrapper<const PeerId>> preferred_peer);
  };

  class BlockLoader {
   public:
    /// Called when all tipset subobjects are available
    /// or tipset appeared to be bad
    using OnBlockSynced = std::function<void(
        const CID &block_cid, outcome::result<BlockHeader> header)>;

    using BlocksAvailable = std::vector<boost::optional<BlockHeader>>;

    outcome::result<BlocksAvailable> loadBlocks(
        const std::vector<CID> &cids,
        boost::optional<std::reference_wrapper<const PeerId>> preferred_peer);

    outcome::result<void> init(
        OnBlockSynced callback,
        std::shared_ptr<storage::indexdb::IndexDb> index_db,
        std::shared_ptr<storage::ipfs::IpfsDatastore> ipfs_db,
        std::shared_ptr<libp2p::protocol::Scheduler> scheduler);

   private:
    // using Wantlist = std::unordered_set<CID>;
    using Wantlist = std::set<CID>;

    struct RequestCtx {
      BlockLoader &owner;

      CID block_cid;

      bool header_wanted = true;
      Wantlist msgs_wantlist;

      boost::optional<BlockMsg> block_msg;

      libp2p::protocol::scheduler::Handle call_completed_;

      RequestCtx(BlockLoader &o, CID c);

      void onHeaderSynced(const BlockMsg &msg);

      void onMessageSynced(const CID &msg_cid);

      void sayCompleted();
    };

    //    void onBlockCompleted(const CID &block_cid, const BlockMsg &msg);

    struct BlockAvailable {
      storage::indexdb::SyncState current_state;
      boost::optional<BlockHeader> header;
    };

    outcome::result<BlockAvailable> findBlock(const CID &cid,
                                              Wantlist &bls_messages_to_load,
                                              Wantlist &secp_messages_to_load);

    outcome::result<void> loadMessages(const std::vector<CID> &cids);

    OnBlockSynced callback_;
    std::shared_ptr<storage::indexdb::IndexDb> index_db_;
    std::shared_ptr<storage::ipfs::IpfsDatastore> ipfs_db_;
    std::shared_ptr<libp2p::protocol::Scheduler> scheduler_;
    bool initialized_ = false;

    std::unordered_map<CID, RequestCtx> block_requests_;

    // friendship with contained objects is ok
    friend RequestCtx;
  };

}  // namespace fc::sync

#endif  // CPP_FILECOIN_SYNC_BLOCK_LOADER_HPP
