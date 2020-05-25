/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef CPP_FILECOIN_STORAGE_INDEXDB_HPP
#define CPP_FILECOIN_STORAGE_INDEXDB_HPP

#include "tipset_hash.hpp"

namespace fc::storage::indexdb {

  using Tipset = primitives::tipset::Tipset;
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

  /// RAII tx helper
  class Tx {
   public:
    explicit Tx(class IndexDb &db);
    void commit();
    void rollback();
    ~Tx();

   private:
    class IndexDb &db_;
    bool done_ = false;
  };

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

    virtual Tx beginTx() = 0;
    virtual void commitTx() = 0;
    virtual void rollbackTx() = 0;

    virtual outcome::result<std::vector<TipsetInfo>> getRoots() = 0;

    virtual outcome::result<std::vector<TipsetInfo>> getHeads() = 0;

    virtual outcome::result<std::pair<uint64_t, SyncState>> getBranchSyncState(
        uint64_t branch_id) = 0;

    virtual outcome::result<void> mergeBranchToHead(uint64_t parent_branch_id,
                                                    uint64_t branch_id) = 0;

    virtual outcome::result<void> splitBranch(uint64_t branch_id,
                                              uint64_t new_head_height,
                                              uint64_t child_branch_id) = 0;

    virtual outcome::result<SyncState> getTipsetSyncState(
        const TipsetHash &tipset_hash) = 0;

    virtual outcome::result<void> updateTipsetSyncState(
        const TipsetHash &tipset_hash) = 0;
  };

}  // namespace fc::storage::indexdb

OUTCOME_HPP_DECLARE_ERROR(fc::storage::indexdb, Error);

#endif  // CPP_FILECOIN_STORAGE_INDEXDB_HPP
