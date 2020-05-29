/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef CPP_FILECOIN_STORAGE_INDEXDB_IMPL_HPP
#define CPP_FILECOIN_STORAGE_INDEXDB_IMPL_HPP

#include "indexdb.hpp"

#include <libp2p/storage/sqlite.hpp>

namespace fc::storage::indexdb {

  auto constexpr kLogName = "indexdb";

  using Blob = std::vector<uint8_t>;

  using GetTipsetFn = std::function<void(const Blob &tipset_hash,
                                         int sync_state,
                                         uint64_t branch_id,
                                         const std::string &weight,
                                         uint64_t height)>;

  using GetCidFn = std::function<void(
      const Blob &cid, const Blob &msg_cid, int type, int sync_state)>;

  using GetBranchFn = std::function<void(uint64_t branch_id)>;

  class IndexDbImpl : public IndexDb {
    using StatementHandle = libp2p::storage::SQLite::StatementHandle;

   public:
    explicit IndexDbImpl(const std::string &db_filename);

    outcome::result<std::vector<TipsetInfo>> getRoots() override;

    outcome::result<std::vector<TipsetInfo>> getHeads() override;

    // root branch + min syncstate of all branches
    outcome::result<std::pair<uint64_t, SyncState>> getBranchSyncState(
        uint64_t branch_id) override;

    outcome::result<void> mergeBranchToHead(uint64_t parent_branch_id,
                                            uint64_t branch_id) override;

    outcome::result<void> splitBranch(uint64_t branch_id,
                                      uint64_t new_head_height,
                                      uint64_t child_branch_id) override;

    outcome::result<void> newTipset(const TipsetHash &tipset_hash,
                                            const std::vector<CID> &block_cids,
                                            const BigInt &weight,
                                            uint64_t height,
                                            uint64_t branch_id) override;

    outcome::result<void> newObject(const CID &cid,
                                            ObjectType type) override;

    outcome::result<void> blockHeaderSynced(
        const CID &block_cid,
        const CID &msg_cid,
        const std::vector<CID> &bls_msgs,
        const std::vector<CID> &secp_msgs) override;

    outcome::result<void> objectSynced(const CID &cid) override;

    outcome::result<SyncState> getTipsetSyncState(
        const TipsetHash &tipset_hash) override;

    outcome::result<SyncState> updateTipsetSyncState(
        const TipsetHash &tipset_hash) override;

   private:
    /// RAII tx helper
    class Tx {
     public:
      explicit Tx(IndexDbImpl &db);
      void commit();
      void rollback();
      ~Tx();

     private:
      IndexDbImpl &db_;
      bool done_ = false;
    };

    [[nodiscard]] Tx beginTx();

    void init();

    outcome::result<std::vector<TipsetInfo>> getRootsOrHeads(
        StatementHandle stmt);

    // parent branch + sync state
    outcome::result<std::pair<uint64_t, int>> getBranchSyncStateStep(
        uint64_t branch_id);

    outcome::result<void> getTipsetInfo(const Blob &tipset_hash,
                                        const GetTipsetFn &cb);

    outcome::result<void> getTipsetCids(const Blob &tipset_hash,
                                        const GetCidFn &cb);

    outcome::result<void> getCidInfo(const Blob &cid, const GetCidFn &cb);

    outcome::result<void> getTipsetsOfCid(const Blob &cid,
                                          const GetTipsetFn &cb);

    outcome::result<void> getParents(
        const Blob &id, const std::function<void(const Blob &)> &cb);

    outcome::result<void> getSuccessors(
        const Blob &id, const std::function<void(const Blob &)> &cb);

    outcome::result<void> getBranches(const GetBranchFn &cb);

//    outcome::result<void> insertBlock(const Blob &cid,
//                                      const Blob &msg_cid,
//                                      int type,
//                                      int sync_state,
//                                      int ref_count);
//
//    outcome::result<void> insertTipset(const Blob &tipset_hash,
//                                       int sync_state,
//                                       uint64_t branch_id,
//                                       const std::string &weight,
//                                       uint64_t height);

    outcome::result<void> insertTipsetBlock(const Blob &tipset_hash,
                                            const Blob &cid,
                                            int seq);

    outcome::result<void> insertLink(const Blob &left, const Blob &right);

    outcome::result<void> linkBranches(uint64_t parent, uint64_t child);

    outcome::result<void> indexBlockMessages(const Blob& block_cid_bytes,
                                             const std::vector<CID> &msgs,
                                             ObjectType msg_type);

    libp2p::storage::SQLite db_;
    StatementHandle get_roots_{};
    StatementHandle get_heads_{};
    StatementHandle get_tipset_info_{};
    StatementHandle get_tipset_cids_{};
    StatementHandle get_cid_info_{};
    StatementHandle get_tipsets_of_cid_{};
    StatementHandle get_parents_{};
    StatementHandle get_parent_branch_{};
    StatementHandle get_successors_{};
    StatementHandle get_branches_{};
    StatementHandle get_branch_sync_state_{};
    StatementHandle get_tipset_sync_state_{};
    StatementHandle insert_cid_{};
    StatementHandle insert_tipset_{};
    StatementHandle insert_tipset_block_{};
    StatementHandle insert_block_msg_{};
    StatementHandle insert_link_{};
    StatementHandle block_header_synced_{};
    StatementHandle update_object_sync_state_{};
    StatementHandle update_tipset_sync_state_{};
    StatementHandle merge_branch_to_head_{};
    StatementHandle link_branches_{};
    StatementHandle unlink_branches_{};
    StatementHandle rename_branch_from_height_{};
    StatementHandle rename_branch_links_{};

    class Test;
    friend Test;
    friend Tx;
  };

}  // namespace fc::storage::indexdb

#endif  // CPP_FILECOIN_STORAGE_INDEXDB_IMPL_HPP
