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
  return outcome::success()

#define RETURN_UPDATE_ONE_ROW(expr)      \
  int rows_affected = (expr);            \
  if (rows_affected != 1) {              \
    return Error::INDEXDB_EXECUTE_ERROR; \
  }                                      \
  return outcome::success()

namespace fc::storage::indexdb {

  namespace {
    auto log() {
      static common::Logger logger = common::createLogger(kLogName);
      return logger.get();
    }

    std::string toString(const CID &cid) {
      auto res = cid.toString();
      if (res) {
        return res.value();
      }
      return "invalid_cid";
    }

    boost::optional<TipsetInfo> decodeTipsetInfo(const Blob &tipset_hash,
                                                 int sync_state,
                                                 uint64_t branch_id,
                                                 const std::string &weight,
                                                 uint64_t height) {
      if (tipset_hash.empty() || (sync_state < SYNC_STATE_BAD)
          || (sync_state > SYNC_STATE_SYNCED) || weight.empty()) {
        log()->error("Bad tipset info for {}", common::hex_lower(tipset_hash));
        return boost::none;
      }
      BigInt w;
      try {
        w = BigInt(weight);
      } catch (...) {
        log()->error("Cannot decode BigInt from {}", weight);
        return boost::none;
      }
      return TipsetInfo{tipset_hash,
                        static_cast<SyncState>(sync_state),
                        branch_id,
                        std::move(w),
                        height};
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

  IndexDbImpl::Tx::Tx(IndexDbImpl &db) : db_(db) {
    db_.db_ << "begin";
  }

  void IndexDbImpl::Tx::commit() {
    if (!done_) {
      done_ = true;
      db_.db_ << "commit";
    }
  }

  void IndexDbImpl::Tx::rollback() {
    if (!done_) {
      done_ = true;
      db_.db_ << "rollback";
    }
  }

  IndexDbImpl::Tx::~Tx() {
    rollback();
  }

  IndexDbImpl::IndexDbImpl(const std::string &db_filename)
      : db_(db_filename, kLogName) {
    init();
  }

  IndexDbImpl::Tx IndexDbImpl::beginTx() {
    return Tx(*this);
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
    auto tx = beginTx();

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

    tx.commit();
    return outcome::success();
  }

  outcome::result<void> IndexDbImpl::splitBranch(uint64_t branch_id,
                                                 uint64_t new_head_height,
                                                 uint64_t child_branch_id) {
    if (child_branch_id == 0 || branch_id == child_branch_id) {
      return Error::INDEXDB_INVALID_ARGUMENT;
    }

    auto tx = beginTx();

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

    tx.commit();
    return outcome::success();
  }

  outcome::result<void> IndexDbImpl::newTipset(
      const TipsetHash &tipset_hash,
      const std::vector<CID> &block_cids,
      const BigInt &weight,
      uint64_t height,
      uint64_t branch_id) {
    auto tx = beginTx();

    int res = db_.execCommand(insert_tipset_,
                              tipset_hash,
                              SYNC_STATE_UNSYNCED,
                              branch_id,
                              weight.str(),
                              height);
    if (res < 0) {
      log()->error("newTipset: cannot insert tipset");
      return Error::INDEXDB_EXECUTE_ERROR;
    }

    if (res == 0) {
      log()->debug("newTipset: tipset {} is already known",
                   common::hex_lower(tipset_hash));
    }

    int seq = 1;
    for (const auto &cid : block_cids) {
      OUTCOME_TRY(cid_bytes, cid.toBytes());

      res = db_.execCommand(
          insert_cid_, cid_bytes, "", OBJECT_TYPE_BLOCK, SYNC_STATE_UNSYNCED);
      if (res < 0) {
        log()->error("newTipset: cannot insert block cid");
        return Error::INDEXDB_EXECUTE_ERROR;
      }
      if (res == 0) {
        log()->debug("newTipset: block {} is already known",
                     common::hex_lower(tipset_hash));
      }

      res = db_.execCommand(insert_tipset_block_, tipset_hash, cid_bytes, seq);
      if (res < 0) {
        log()->error("newTipset: cannot link {} and {}",
                     common::hex_lower(tipset_hash),
                     toString(cid));
        return Error::INDEXDB_EXECUTE_ERROR;
      }
      ++seq;
    }

    tx.commit();
    return outcome::success();
  }

  outcome::result<void> IndexDbImpl::newObject(const CID &cid,
                                               ObjectType type) {
    OUTCOME_TRY(cid_bytes, cid.toBytes());
    int res = db_.execCommand(
        insert_cid_, cid_bytes, Blob(), type, SYNC_STATE_UNSYNCED);
    if (res < 0) {
      return Error::INDEXDB_EXECUTE_ERROR;
    }
    if (res == 0) {
      log()->debug("cid {} is already known", toString(cid));
    }
    return outcome::success();
  }

  outcome::result<void> IndexDbImpl::blockHeaderSynced(
      const CID &block_cid,
      const CID &msg_cid,
      const std::vector<CID> &bls_msgs,
      const std::vector<CID> &secp_msgs) {
    OUTCOME_TRY(block_cid_bytes, block_cid.toBytes());
    OUTCOME_TRY(msg_cid_bytes, msg_cid.toBytes());

    auto tx = beginTx();

    int res = db_.execCommand(block_header_synced_,
                              block_cid_bytes,
                              msg_cid_bytes,
                              OBJECT_TYPE_BLOCK,
                              SYNC_STATE_HEADER_SYNCED);
    if (res < 0) {
      log()->error("blockHeaderSynced: cannot update block {}",
                   toString(block_cid));
      return Error::INDEXDB_EXECUTE_ERROR;
    }

    OUTCOME_TRY(
        indexBlockMessages(block_cid_bytes, bls_msgs, OBJECT_TYPE_BLS_MESSAGE));
    OUTCOME_TRY(indexBlockMessages(
        block_cid_bytes, secp_msgs, OBJECT_TYPE_SECP_MESSAGE));

    tx.commit();
    return outcome::success();
  }

  outcome::result<void> IndexDbImpl::indexBlockMessages(
      const Blob &block_cid_bytes,
      const std::vector<CID> &msgs,
      ObjectType msg_type) {
    int seq = 1;
    if (msg_type == OBJECT_TYPE_SECP_MESSAGE) {
      seq += 1000000000;
    }

    for (const auto &cid : msgs) {
      OUTCOME_TRY(cid_bytes, cid.toBytes());

      int res = db_.execCommand(insert_cid_,
                                cid_bytes,
                                block_cid_bytes,
                                msg_type,
                                SYNC_STATE_UNSYNCED);
      if (res < 0) {
        log()->error("indexBlockMessages: cannot insert msg cid");
        return Error::INDEXDB_EXECUTE_ERROR;
      }
      if (res == 0) {
        log()->debug("indexBlockMessages: msg {} is already known",
                     toString(cid));
      }

      res = db_.execCommand(insert_block_msg_, block_cid_bytes, cid_bytes, seq);
      if (res < 0) {
        log()->error("indexBlockMessages: cannot link msg cid {}",
                     toString(cid));
        return Error::INDEXDB_EXECUTE_ERROR;
      }
      ++seq;
    }

    return outcome::success();
  }

  outcome::result<void> IndexDbImpl::objectSynced(const CID &cid) {
    OUTCOME_TRY(cid_bytes, cid.toBytes());
    RETURN_UPDATE_ONE_ROW(db_.execCommand(
        update_object_sync_state_, SYNC_STATE_SYNCED, cid_bytes));
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

  outcome::result<TipsetInfo> IndexDbImpl::getTipsetInfo(
      const TipsetHash &tipset_hash) {
    boost::optional<TipsetInfo> info;
    bool res = db_.execQuery(
        get_tipset_info_,
        [&](const Blob &tipset_hash,
            int sync_state,
            uint64_t branch_id,
            const std::string &weight,
            uint64_t height) {
          info = decodeTipsetInfo(
              tipset_hash, sync_state, branch_id, weight, height);
        },
        tipset_hash);
    if (!info) {
      return Error::INDEXDB_NOT_FOUND;
    }
    if (!res) {
      return Error::INDEXDB_EXECUTE_ERROR;
    }
    return info.value();
  }

  outcome::result<SyncState> IndexDbImpl::updateTipsetSyncState(
      const TipsetHash &tipset_hash,
      boost::optional<const std::reference_wrapper<TipsetHash>> parent) {
    auto tx = beginTx();

    int res = 0;
    if (parent) {
      res = db_.execCommand(insert_link_, parent.value().get(), tipset_hash);
    }
    if (res >= 0) {
      res =
          db_.execCommand(update_tipset_sync_state_, tipset_hash, tipset_hash);
    }
    if (res < 0) {
      return Error::INDEXDB_EXECUTE_ERROR;
    }
    auto state = getTipsetSyncState(tipset_hash);
    if (!state) {
      return state;
    }
    if (state.value() == SYNC_STATE_UNKNOWN) {
      return Error::INDEXDB_NOT_FOUND;
    }

    tx.commit();
    return state;
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

  outcome::result<void> IndexDbImpl::insertTipsetBlock(const Blob &tipset_hash,
                                                       const Blob &cid,
                                                       int seq) {
    RETURN_UPDATE_ONE_ROW(
        db_.execCommand(insert_tipset_block_, tipset_hash, cid, seq));
  }

  outcome::result<void> IndexDbImpl::linkBranches(uint64_t parent,
                                                  uint64_t child) {
    RETURN_UPDATE_ONE_ROW(db_.execCommand(link_branches_, parent, parent));
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
            ref_cid BLOB NOT NULL,
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
            UNIQUE (tipset, cid)))",
        R"(CREATE UNIQUE INDEX IF NOT EXISTS tipset_seq ON blocks_in_tipset
            (tipset, seq))",
        R"(CREATE TABLE IF NOT EXISTS messages_in_block (
            block BLOB REFERENCES cids(cid),
            msg BLOB REFERENCES cids(cid),
            seq INTEGER NOT NULL,
            UNIQUE (block, msg)))",
        R"(CREATE UNIQUE INDEX IF NOT EXISTS msg_seq ON messages_in_block
            (block, seq))",
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
          R"(SELECT cid,ref_cid,type,sync_state FROM cids
          WHERE cid IN
          (SELECT cid FROM blocks_in_tipset WHERE tipset=? ORDER BY seq))");

      get_cid_info_ = db_.createStatement(
          R"(SELECT cid,ref_cid,type,sync_state FROM cids
          WHERE cid=?)");

      get_tipsets_of_cid_ = db_.createStatement(
          R"(SELECT hash,sync_state,branch_id,weight,height FROM tipsets
          WHERE hash IN(SELECT tipset FROM blocks_in_tipset WHERE cid=?))");

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

      insert_cid_ =
          db_.createStatement("INSERT OR IGNORE INTO cids VALUES(?,?,?,?)");

      insert_tipset_ = db_.createStatement(
          "INSERT OR IGNORE INTO tipsets VALUES(?,?,?,?,?)");

      insert_tipset_block_ =
          db_.createStatement("INSERT INTO blocks_in_tipset VALUES(?,?,?)");

      insert_block_msg_ =
          db_.createStatement("INSERT INTO messages_in_block VALUES(?,?,?)");

      insert_link_ =
          db_.createStatement("INSERT OR IGNORE INTO links VALUES(?,?)");

      block_header_synced_ =
          db_.createStatement("INSERT OR REPLACE INTO cids VALUES(?,?,?,?)");

      update_object_sync_state_ =
          db_.createStatement("UPDATE cids SET sync_state=? WHERE cid=?");

      update_tipset_sync_state_ = db_.createStatement(
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
