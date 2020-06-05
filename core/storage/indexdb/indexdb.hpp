/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef CPP_FILECOIN_STORAGE_INDEXDB_HPP
#define CPP_FILECOIN_STORAGE_INDEXDB_HPP

#include "primitives/tipset/tipset_key.hpp"
#include "primitives/big_int.hpp"

namespace fc::storage::indexdb {

  using TipsetKey = primitives::tipset::TipsetKey;
  using TipsetHash = primitives::tipset::TipsetHash;
  using primitives::BigInt;

  enum class Error {
    INDEXDB_CANNOT_CREATE = 1,
    INDEXDB_NOT_FOUND,
    INDEXDB_INVALID_ARGUMENT,
    INDEXDB_EXECUTE_ERROR,
    INDEXDB_DECODE_ERROR
  };

  outcome::result<std::shared_ptr<class IndexDb>> createIndexDb(
      const std::string &db_filename);

  enum SyncState {
    SYNC_STATE_BAD = -1,
    SYNC_STATE_UNKNOWN = 0,
    SYNC_STATE_UNSYNCED = 1,
    SYNC_STATE_HEADER_SYNCED = 2,
    SYNC_STATE_SYNCED = 3,
  };

  enum ObjectType {
    OBJECT_TYPE_UNKNOWN = 0,
    OBJECT_TYPE_BLOCK = 1,
    OBJECT_TYPE_BLS_MESSAGE = 2,
    OBJECT_TYPE_SECP_MESSAGE = 3,
  };

  struct TipsetInfo {
    TipsetHash tipset;
    SyncState sync_state = SYNC_STATE_UNKNOWN;
    uint64_t branch_id = 0;
    BigInt weight;
    uint64_t height = 0;
  };

  struct CIDInfo {
    CID cid;
    CID msg_cid;
    ObjectType type = OBJECT_TYPE_UNKNOWN;
    SyncState sync_state = SYNC_STATE_UNKNOWN;
  };

  class IndexDb {
   public:
    virtual ~IndexDb() = default;

    virtual outcome::result<std::vector<TipsetInfo>> getRoots() = 0;

    virtual outcome::result<std::vector<TipsetInfo>> getHeads() = 0;

    virtual outcome::result<std::pair<uint64_t, SyncState>> getBranchSyncState(
        uint64_t branch_id) = 0;

    virtual outcome::result<void> mergeBranchToHead(uint64_t parent_branch_id,
                                                    uint64_t branch_id) = 0;

    virtual outcome::result<void> splitBranch(uint64_t branch_id,
                                              uint64_t new_head_height,
                                              uint64_t child_branch_id) = 0;

    /// Adds info about a new unsynced tipset. Called when new head info arrives
    /// from network
    virtual outcome::result<void> newTipset(const TipsetHash &tipset_hash,
                                            const std::vector<CID> &block_cids,
                                            const BigInt &weight,
                                            uint64_t height,
                                            uint64_t branch_id) = 0;

    /// updates tipset sync state and returns it
    virtual outcome::result<SyncState> getTipsetSyncState(
        const TipsetHash &tipset_hash) = 0;

    virtual outcome::result<TipsetInfo> getTipsetInfo(
        const TipsetHash &tipset_hash) = 0;

    /// returns tipset sync state
    virtual outcome::result<SyncState> updateTipsetSyncState(
        const TipsetHash &tipset_hash,
        boost::optional<const std::reference_wrapper<TipsetHash>> parent) = 0;

    /// Adds info about new block or message CID, unsynced
    virtual outcome::result<void> newObject(const CID &cid,
                                            ObjectType type) = 0;

    virtual outcome::result<void> blockHeaderSynced(
        const CID &block_cid,
        const CID &msg_cid,
        const std::vector<CID> &bls_msgs,
        const std::vector<CID> &secp_msgs) = 0;

    virtual outcome::result<void> objectSynced(const CID &cid) = 0;
  };

}  // namespace fc::storage::indexdb

OUTCOME_HPP_DECLARE_ERROR(fc::storage::indexdb, Error);

#endif  // CPP_FILECOIN_STORAGE_INDEXDB_HPP
