/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "indexdb_impl.hpp"

#include "common/logger.hpp"

#define RETURN_QUERY_RESULT(expr)        \
  bool res = (expr);                     \
  if (!res) {                            \
    return Error::INDEXDB_EXECUTE_ERROR; \
  }                                      \
  return outcome::success();

#define RETURN_UPDATE_ONE_ROW(expr)      \
  int rows_affected = (expr);            \
  if (rows_affected != 1) {              \
    return Error::INDEXDB_EXECUTE_ERROR; \
  }                                      \
  return outcome::success();

namespace fc::storage::indexdb {

  namespace {
    auto log() {
      static common::Logger logger = common::createLogger(kLogName);
      return logger.get();
    }

    bool addTipsetInfo(std::vector<TipsetInfo> &tipsets,
                       const Blob &tipset_hash,
                       int sync_state,
                       uint64_t branch_id,
                       const std::string &weight,
                       uint64_t height) {
      if (tipset_hash.empty() || (sync_state < SYNC_STATE_BAD)
          || (sync_state > SYNC_STATE_SYNCED) || weight.empty()) {
        log()->error("Bad tipset info for {}", common::hex_lower(tipset_hash));
        return false;
      }
      BigInt w;
      try {
        w = BigInt(weight);
      } catch (...) {
        log()->error("Cannot decode BigInt from {}", weight);
        return false;
      }
      tipsets.emplace_back(TipsetInfo{tipset_hash,
                                      static_cast<SyncState>(sync_state),
                                      branch_id,
                                      std::move(w),
                                      height});
      return true;
    }

    outcome::result<SyncState> decodeTipsetSyncState(int int_val) {
      if ((int_val < SYNC_STATE_BAD) || (int_val > SYNC_STATE_SYNCED)) {
        return Error::INDEXDB_DECODE_ERROR;
      }
      return static_cast<SyncState>(int_val);
    }

  }  // namespace

  Tx::Tx(IndexDb &db) : db_(db) {}

  void Tx::commit() {
    db_.commitTx();
    done_ = true;
  }

  void Tx::rollback() {
    db_.rollbackTx();
    done_ = true;
  }

  Tx::~Tx() {
    if (!done_) db_.rollbackTx();
  }

  IndexDbImpl::IndexDbImpl(const std::string &db_filename)
      : db_(db_filename, kLogName) {
    init();
  }

  Tx IndexDbImpl::beginTx() {
    db_ << "begin";
    return Tx(*this);
  }

  void IndexDbImpl::commitTx() {
    db_ << "commit";
  }

  void IndexDbImpl::rollbackTx() {
    db_ << "rollback";
  }

  outcome::result<std::vector<TipsetInfo>> IndexDbImpl::getRoots() {
    return getRootsOrHeads(get_roots_);
  }

  outcome::result<std::vector<TipsetInfo>> IndexDbImpl::getHeads() {
    return getRootsOrHeads(get_heads_);
  }

  outcome::result<std::vector<TipsetInfo>> IndexDbImpl::getRootsOrHeads(
      StatementHandle stmt) {
    std::vector<TipsetInfo> tipsets;
    bool decode_success = true;
    bool query_success = db_.execQuery(
        stmt,
        [&](const Blob &tipset_hash,
            int sync_state,
            uint64_t branch_id,
            const std::string &weight,
            uint64_t height) {
          if (!decode_success) {
            return;
          }
          decode_success = addTipsetInfo(
              tipsets, tipset_hash, sync_state, branch_id, weight, height);
        });
    if (!query_success) {
      return Error::INDEXDB_EXECUTE_ERROR;
    }
    if (!decode_success) {
      return Error::INDEXDB_DECODE_ERROR;
    }
    return tipsets;
  }

  outcome::result<std::pair<uint64_t, SyncState>>
  IndexDbImpl::getBranchSyncState(uint64_t branch_id) {
    uint64_t root_id = branch_id;
    int state_int = INT_MAX;

    for (;;) {
      OUTCOME_TRY(p, getBranchSyncStateStep(branch_id));

      if (p.second < state_int) {
        state_int = p.second;
      }

      if (p.first == 0) {
        break;
      } else {
        root_id = p.first;
      }
    }

    OUTCOME_TRY(state, decodeTipsetSyncState(state_int));
    return std::pair(root_id, state);
  }

  outcome::result<std::pair<uint64_t, int>> IndexDbImpl::getBranchSyncStateStep(
      uint64_t branch_id) {
    int state_int = INT_MIN;
    bool res = db_.execQuery(
        get_branch_sync_state_, [&](int s) { state_int = s; }, branch_id);
    if (!res) {
      return Error::INDEXDB_EXECUTE_ERROR;
    }
    if (state_int == INT_MIN) {
      return Error::INDEXDB_NOT_FOUND;
    }

    uint64_t parent_id = 0;

    if (state_int > SYNC_STATE_UNKNOWN) {
      res = db_.execQuery(
          get_parent_branch_,
          [&parent_id](uint64_t id) { parent_id = id; },
          branch_id);
      if (!res) {
        return Error::INDEXDB_EXECUTE_ERROR;
      }
    }
    return std::pair(parent_id, state_int);
  }

  outcome::result<void> IndexDbImpl::mergeBranchToHead(
      uint64_t parent_branch_id, uint64_t branch_id) {
    beginTx();

    int rows = db_.execCommand(unlink_branches_, parent_branch_id, branch_id);

    if (rows != 1) {
      log()->error(
          "branch {} is not a parent of {}", parent_branch_id, branch_id);
      return Error::INDEXDB_INVALID_ARGUMENT;
    }

    rows = db_.execCommand(merge_branch_to_head_, parent_branch_id, branch_id);
    if (rows == 0) {
      // ~~~ log
      return Error::INDEXDB_NOT_FOUND;
    }
    if (rows < 0) {
      // ~~~ log
      return Error::INDEXDB_EXECUTE_ERROR;
    }

    commitTx();
    return outcome::success();
  }

  outcome::result<void> IndexDbImpl::splitBranch(uint64_t branch_id,
                                                 uint64_t new_head_height,
                                                 uint64_t child_branch_id) {
    if (child_branch_id == 0 || branch_id == child_branch_id) {
      return Error::INDEXDB_INVALID_ARGUMENT;
    }

    beginTx();

    int rows = db_.execCommand(rename_branch_from_height_,
                               child_branch_id,
                               branch_id,
                               new_head_height);

    if (rows == 0) {
      // ~~~ log
      return Error::INDEXDB_NOT_FOUND;
    }
    if (rows < 0) {
      // ~~~ log
      return Error::INDEXDB_EXECUTE_ERROR;
    }

    rows = db_.execCommand(rename_branch_links_, child_branch_id, branch_id);
    if (rows < 0) {
      // ~~~ log
      return Error::INDEXDB_EXECUTE_ERROR;
    }

    commitTx();
    return outcome::success();
  }

  outcome::result<SyncState> IndexDbImpl::getTipsetSyncState(
      const Blob &tipset_hash) {
    int state = INT_MIN;
    bool res = db_.execQuery(
        get_tipset_sync_state_, [&](int s) { state = s; }, tipset_hash);
    if (!res) {
      return Error::INDEXDB_EXECUTE_ERROR;
    }
    if (state == INT_MIN) {
      return Error::INDEXDB_NOT_FOUND;
    }
    return decodeTipsetSyncState(state);
  }

  outcome::result<void> IndexDbImpl::updateTipsetSyncState(
      const TipsetHash &tipset_hash) {
    RETURN_UPDATE_ONE_ROW(db_.execCommand(
        update_tipset_sync_state_, tipset_hash, tipset_hash));
  }

  outcome::result<void> IndexDbImpl::getTipsetInfo(const Blob &tipset_hash,
                                                   const GetTipsetFn &cb) {
    RETURN_QUERY_RESULT(db_.execQuery(get_tipset_info_, cb, tipset_hash));
  }

  outcome::result<void> IndexDbImpl::getTipsetCids(const Blob &tipset_hash,
                                                   const GetCidFn &cb) {
    RETURN_QUERY_RESULT(db_.execQuery(get_tipset_cids_, cb, tipset_hash));
  }

  outcome::result<void> IndexDbImpl::getCidInfo(const Blob &cid,
                                                const GetCidFn &cb) {
    RETURN_QUERY_RESULT(db_.execQuery(get_cid_info_, cb, cid));
  }

  outcome::result<void> IndexDbImpl::getTipsetsOfCid(const Blob &cid,
                                                     const GetTipsetFn &cb) {
    RETURN_QUERY_RESULT(db_.execQuery(get_tipsets_of_cid_, cb, cid));
  }

  outcome::result<void> IndexDbImpl::getParents(
      const Blob &id, const std::function<void(const Blob &)> &cb) {
    RETURN_QUERY_RESULT(db_.execQuery(get_parents_, cb, id));
  }

  outcome::result<void> IndexDbImpl::getSuccessors(
      const Blob &id, const std::function<void(const Blob &)> &cb) {
    RETURN_QUERY_RESULT(db_.execQuery(get_successors_, cb, id));
  }

  outcome::result<void> IndexDbImpl::getBranches(const GetBranchFn &cb) {
    RETURN_QUERY_RESULT(db_.execQuery(get_branches_, cb));
  }

  outcome::result<void> IndexDbImpl::insertBlock(const Blob &cid,
                                                 const Blob &msg_cid,
                                                 int type,
                                                 int sync_state,
                                                 int ref_count) {
    RETURN_UPDATE_ONE_ROW(db_.execCommand(
        insert_block_, cid, msg_cid, type, sync_state, ref_count));
  }

  outcome::result<void> IndexDbImpl::insertTipset(const Blob &tipset_hash,
                                                  int sync_state,
                                                  uint64_t branch_id,
                                                  const std::string &weight,
                                                  uint64_t height) {
    RETURN_UPDATE_ONE_ROW(db_.execCommand(
        insert_tipset_, tipset_hash, sync_state, branch_id, weight, height));
  }

  outcome::result<void> IndexDbImpl::insertTipsetBlock(const Blob &tipset_hash,
                                                       const Blob &cid,
                                                       int seq) {
    RETURN_UPDATE_ONE_ROW(
        db_.execCommand(insert_tipset_block_, tipset_hash, cid, seq));
  }

  outcome::result<void> IndexDbImpl::insertLink(const Blob &left,
                                                const Blob &right) {
    RETURN_UPDATE_ONE_ROW(db_.execCommand(insert_link_, left, right));
  }

  outcome::result<void> IndexDbImpl::updateBlockSyncState(const Blob &cid,
                                                          int new_sync_state) {
    RETURN_UPDATE_ONE_ROW(
        db_.execCommand(update_block_sync_state_, new_sync_state, cid));
  }

  void IndexDbImpl::init() {
    static const char *schema[] = {
        R"(CREATE TABLE IF NOT EXISTS tipsets (
            hash BLOB PRIMARY KEY,
            sync_state INTEGER NOT NULL,
            branch_id INTEGER NOT NULL,
            weight TEXT NOT NULL,
            height INTEGER NOT NULL))",
        R"(CREATE INDEX IF NOT EXISTS tipsets_h ON tipsets
            (height))",
        R"(CREATE TABLE IF NOT EXISTS cids (
            cid BLOB PRIMARY KEY,
            msg_cid BLOB NOT_NULL,
            type INTEGER NOT NULL,
            sync_state INTEGER NOT NULL))",
        R"(CREATE TABLE IF NOT EXISTS links (
            left BLOB NOT NULL,
            right BLOB NOT NULL,
            UNIQUE (left, right)))",
        R"(CREATE INDEX IF NOT EXISTS links_l ON links
            (left))",
        R"(CREATE INDEX IF NOT EXISTS links_r ON links
            (right))",
        R"(CREATE TABLE IF NOT EXISTS branch_links (
            parent INTEGER NOT NULL,
            child INTEGER NOT NULL,
            UNIQUE (parent, child)))",
        R"(CREATE INDEX IF NOT EXISTS blinks_l ON branch_links
            (parent))",
        R"(CREATE INDEX IF NOT EXISTS blinks_r ON branch_links
            (child))",
        R"(CREATE TABLE IF NOT EXISTS blocks_in_tipset (
            tipset BLOB REFERENCES tipsets(hash),
            cid BLOB REFERENCES cids(cid),
            seq INTEGER NOT NULL,
            UNIQUE (tipset, cid, seq)))",
    };

    try {
      db_ << "PRAGMA foreign_keys = ON";
      auto tx = beginTx();

      for (auto sql : schema) {
        db_ << sql;
      }

      get_roots_ = db_.createStatement(
          R"(SELECT hash,sync_state,branch_id,weight,height FROM tipsets
          WHERE NOT EXISTS(SELECT left FROM links WHERE right=hash))");

      get_heads_ = db_.createStatement(
          R"(SELECT hash,sync_state,branch_id,weight,height FROM tipsets
          WHERE NOT EXISTS(SELECT right FROM links WHERE left=hash))");

      get_tipset_info_ = db_.createStatement(
          R"(SELECT hash,sync_state,branch_id,weight,height FROM tipsets
          WHERE hash=?)");

      get_tipset_cids_ = db_.createStatement(
          R"(SELECT cid,msg_cid,type,sync_state FROM cids
          WHERE cid IN
          (SELECT cid FROM blocks_in_tipset WHERE tipset=? ORDER BY seq))");

      get_cid_info_ = db_.createStatement(
          R"(SELECT cid,msg_cid,type,sync_state FROM cids
          WHERE cid=?)");

      get_tipsets_of_cid_ = db_.createStatement(
          R"(SELECT hash,sync_state,branch_id,weight,height FROM tipsets
          WHERE hash IN(SELECT tipsets FROM blocks_in_tipset WHERE cid=?))");

      get_parents_ =
          db_.createStatement("SELECT left FROM links WHERE right=?");

      get_parent_branch_ =
          db_.createStatement(R"(SELECT parent FROM branch_links
          WHERE child=? LIMIT 1)");

      get_successors_ =
          db_.createStatement("SELECT right FROM links WHERE left=?");

      get_branches_ =
          db_.createStatement("SELECT DISTINCT branch_id FROM tipsets");

      get_branch_sync_state_ = db_.createStatement(
          "SELECT MIN(sync_state) FROM tipsets WHERE branch_id=?");

      get_tipset_sync_state_ = db_.createStatement(
          R"(SELECT min(sync_state) FROM cids
          WHERE cid IN
          (SELECT cid FROM blocks_in_tipset WHERE tipset=?))");

      insert_block_ =
          db_.createStatement("INSERT OR IGNORE INTO cids VALUES(?,?,?,?,?)");

      insert_tipset_ = db_.createStatement(
          "INSERT OR IGNORE INTO tipsets VALUES(?,?,?,?,?)");

      insert_tipset_block_ =
          db_.createStatement("INSERT INTO blocks_in_tipset VALUES(?,?,?)");

      insert_link_ = db_.createStatement("INSERT INTO links VALUES(?,?)");

      update_block_sync_state_ =
          db_.createStatement("UPDATE cids SET sync_state=? WHERE cid=?");

      update_tipset_sync_state_ =
          db_.createStatement(
              R"(UPDATE tipsets SET sync_state=
              (SELECT MIN(sync_state) FROM cids WHERE cid IN
              (SELECT cid FROM blocks_in_tipset WHERE tipset=?))
              WHERE hash=?)");

      merge_branch_to_head_ = db_.createStatement(
          "UPDATE tipsets SET branch_id=? WHERE branch_id=?");

      link_branches_ =
          db_.createStatement("INSERT INTO branch_links VALUES(?,?)");

      unlink_branches_ = db_.createStatement(
          "DELETE FROM branch_links WHERE parent=? AND child=?");

      rename_branch_from_height_ = db_.createStatement(
          "UPDATE tipsets SET branch_id=? WHERE branch_id=? AND height>?");

      rename_branch_links_ = db_.createStatement(
          "UPDATE branch_links SET parent=? WHERE parent=?");

      tx.commit();
    } catch (sqlite::sqlite_exception &e) {
      throw std::runtime_error(e.get_sql());
    }
  }

  outcome::result<std::shared_ptr<IndexDb>> createIndexDb(
      const std::string &db_filename) {
    try {
      return std::make_shared<IndexDbImpl>(db_filename);
    } catch (const std::exception &e) {
      log()->error("cannot create index db ({}): {}", db_filename, e.what());
    } catch (...) {
      log()->error("cannot create index db ({}): exception", db_filename);
    }
    return Error::INDEXDB_CANNOT_CREATE;
  }

}  // namespace fc::storage::indexdb

OUTCOME_CPP_DEFINE_CATEGORY(fc::storage::indexdb, Error, e) {
  using E = fc::storage::indexdb::Error;
  switch (e) {
    case E::INDEXDB_CANNOT_CREATE:
      return "indexdb: cannot open db";
    case E::INDEXDB_INVALID_ARGUMENT:
      return "indexdb: invalid argument";
    case E::INDEXDB_EXECUTE_ERROR:
      return "indexdb: query execute error";
    case E::INDEXDB_DECODE_ERROR:
      return "indexdb: decode error";
    default:
      break;
  }
  return "indexdb: unknown error";
}
