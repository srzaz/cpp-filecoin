/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef CPP_FILECOIN_SYNC_TIPSET_LOADER_HPP
#define CPP_FILECOIN_SYNC_TIPSET_LOADER_HPP

#include <set>

#include <libp2p/protocol/common/scheduler.hpp>

#include "common.hpp"
#include "primitives/tipset/tipset.hpp"
#include "storage/indexdb/indexdb.hpp"
#include "storage/ipfs/datastore.hpp"

namespace fc::sync {

  class BlockLoader {
   public:
    using BlocksAvailable = std::vector<boost::optional<BlockHeader>>;

    outcome::result<BlocksAvailable> loadBlocks(
        const TipsetKey &key,
        boost::optional<std::reference_wrapper<const PeerId>> preferred_peer);
  };

  class TipsetLoader {
   public:
    /// Called when all tipset subobjects are available
    /// or tipset appeared to be bad
    using OnTipset = std::function<void(const TipsetKey &key,
                                        outcome::result<Tipset> tipset)>;

    outcome::result<void> init(
        OnTipset callback,
        std::shared_ptr<BlockLoader> block_loader,
        std::shared_ptr<storage::indexdb::IndexDb> index_db,
        std::shared_ptr<storage::ipfs::IpfsDatastore> ipfs_db,
        std::shared_ptr<libp2p::protocol::Scheduler> scheduler);

    /// Returns immediately if tipset is availabe locally, otherwise
    /// begins synchronizing its subobjects from the network
    outcome::result<boost::optional<Tipset>> loadTipset(
        const TipsetKey &key,
        boost::optional<std::reference_wrapper<const PeerId>> preferred_peer);

   private:
    using Wantlist = std::set<CID>;

    struct RequestCtx {
      TipsetLoader &owner;

      TipsetKey tipset_key;

      // block cids we are waiting for
      Wantlist wantlist;

      // the puzzle being filled
      std::vector<boost::optional<BlockHeader>> blocks_filled;

      bool is_bad_tipset = false;

      libp2p::protocol::scheduler::Handle call_completed;

      RequestCtx(TipsetLoader &o, const TipsetKey &key);

      void onBlockSynced(const CID &cid, const BlockHeader &bh);

      void onCidIsNotABlock(const CID &cid);
    };

    void onRequestCompleted(outcome::result<Tipset> tipset,
                            const TipsetKey &tipset_key);

    OnTipset callback_;
    std::shared_ptr<BlockLoader> block_loader_;
    std::shared_ptr<storage::indexdb::IndexDb> index_db_;
    std::shared_ptr<storage::ipfs::IpfsDatastore> ipfs_db_;
    std::shared_ptr<libp2p::protocol::Scheduler> scheduler_;
    std::map<TipsetHash, RequestCtx> tipset_requests_;
    Wantlist global_wantlist_;
    bool initialized_ = false;

    // friendship with contained objects is ok
    friend RequestCtx;
  };

}  // namespace fc::sync

#endif  // CPP_FILECOIN_SYNC_TIPSET_LOADER_HPP
