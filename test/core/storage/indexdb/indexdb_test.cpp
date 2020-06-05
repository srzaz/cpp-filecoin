/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <stdio.h>

#include <gtest/gtest.h>
#include <spdlog/fmt/fmt.h>
#include <boost/filesystem.hpp>

#include "storage/indexdb/indexdb.hpp"
#include "testutil/outcome.hpp"

using namespace fc::storage::indexdb;
namespace fs = boost::filesystem;

namespace {
  using Blob = std::vector<uint8_t>;
  using BlobGraph = std::vector<std::vector<Blob>>;
  using BranchGraph = std::vector<std::vector<uint64_t>>;
  using Cids = std::vector<fc::CID>;

  constexpr auto kMemoryDbName = ":memory:";

  Blob createBlob(size_t n, int i) {
    uint64_t x = (n << 16) + i;
    auto p = (const uint8_t *)&x;
    return Blob(p, p + 8);
  }

  fc::CID createCID(size_t n, int i) {
    uint64_t x = (n << 16) + i;
    auto p = (const uint8_t *)&x;
    EXPECT_OUTCOME_TRUE_2(cid, fc::common::getCidOf(gsl::span(p, 8)));
    return cid;
  }

  uint64_t createBranchId(size_t n, int i) {
    return (n << 16) + i;
  }

  template <class G>
  G createGraph(std::initializer_list<int> nodes_per_layers) {
    G g;
    g.reserve(nodes_per_layers.size());
    for (auto n : nodes_per_layers) {
      g.emplace_back();
      auto &v = g.back();
      v.reserve(n);
      for (int i = 0; i < n; ++i) {
        if constexpr (std::is_same<G, BranchGraph>::value) {
          v.push_back(createBranchId(g.size(), i));
        } else {
          v.push_back(createBlob(g.size(), i));
        }
      }
    }
    return g;
  }

  template <class G>
  void insertGraph(IndexDb &db, const G &g) {
    EXPECT_GE(g.size(), 2);

    // auto tx = db.beginTx();

    for (auto n_layer = 0u; n_layer < g.size() - 1; ++n_layer) {
      const auto &parents = g[n_layer];
      const auto &children = g[n_layer + 1];
      for (const auto &p : parents) {
        for (const auto &c : children) {
          // TODO         EXPECT_OUTCOME_TRUE_1(setParent(db, p, c));
        }
      }
    }

    // tx.commit();
  }

  template <class C, class E>
  bool contains(const C &container, const E &element) {
    return std::find(container.begin(), container.end(), element)
           != container.end();
  }

  struct Unlink {
    std::string file_name_;
    explicit Unlink(const std::string &file_name) : file_name_(file_name) {
      do_unlink();
    }
    ~Unlink() {
      do_unlink();
    }
    void do_unlink() {
      remove(file_name_.c_str());
    }
  };

  std::shared_ptr<IndexDb> createEmptyDb(const std::string &file_name) {
    EXPECT_OUTCOME_TRUE_2(db, createIndexDb(file_name));
    EXPECT_OUTCOME_TRUE_2(roots, db->getRoots());
    EXPECT_TRUE(roots.empty());
    return db;
  }

  std::pair<TipsetInfo, Cids> createNewTipset(size_t n_blocks,
                                              SyncState sync_state,
                                              BigInt weight,
                                              uint64_t height,
                                              uint64_t branch_id,
                                              size_t blocks_unique_prefix = 0) {
    std::pair<TipsetInfo, Cids> ret;
    auto &cids = ret.second;
    cids.reserve(n_blocks);
    for (size_t i = 0; i < n_blocks; ++i) {
      cids.emplace_back(createCID(blocks_unique_prefix, i));
    }
    TipsetInfo &i = ret.first;
    i.tipset = fc::primitives::tipset::tipsetHash(cids);
    i.sync_state = sync_state;
    i.branch_id = branch_id;
    i.weight = std::move(weight);
    i.height = height;
    return ret;
  }

  std::tuple<fc::CID, Cids, Cids> createMessageCids(size_t n_bls_messages,
                                                    size_t n_secp_messages) {
    static size_t unique_seed = 1 << 20;

    auto createUniqueCid = [&]() -> fc::CID {
      return createCID(++unique_seed, 1);
    };

    Cids bls_msgs;
    bls_msgs.reserve(n_bls_messages);
    std::generate_n(
        std::back_inserter(bls_msgs), n_bls_messages, createUniqueCid);

    Cids secp_msgs;
    secp_msgs.reserve(n_secp_messages);
    std::generate_n(
        std::back_inserter(secp_msgs), n_secp_messages, createUniqueCid);

    return std::make_tuple(
        createUniqueCid(), std::move(bls_msgs), std::move(secp_msgs));
  }

  void insertUnsyncedTipset(IndexDb &db,
                            const TipsetInfo &tipset,
                            const Cids &cids) {
    EXPECT_OUTCOME_TRUE_1(db.newTipset(
        tipset.tipset, cids, tipset.weight, tipset.height, tipset.branch_id));
  }

  void makeBlockSynced(IndexDb &db,
                       const fc::CID &block_cid,
                       const fc::CID &msg_cid,
                       const Cids &bls_msgs,
                       const Cids &secp_msgs) {
    EXPECT_OUTCOME_TRUE_1(
        db.blockHeaderSynced(block_cid, msg_cid, bls_msgs, secp_msgs));
    for (const auto &cid : bls_msgs) {
      EXPECT_OUTCOME_TRUE_1(db.objectSynced(cid));
    }
    for (const auto &cid : secp_msgs) {
      EXPECT_OUTCOME_TRUE_1(db.objectSynced(cid));
    }
    EXPECT_OUTCOME_TRUE_1(db.objectSynced(block_cid));
  }

  void expectEqual(const TipsetInfo &a, const TipsetInfo &b) {
    EXPECT_EQ(a.tipset, b.tipset);
    EXPECT_EQ(a.sync_state, b.sync_state);
    EXPECT_EQ(a.branch_id, b.branch_id);
    EXPECT_EQ(a.weight, b.weight);
    EXPECT_EQ(a.height, b.height);
  }

  void testCreationReopening(const std::string &file_name, bool reopen) {
    auto db = createEmptyDb(file_name);

    auto [tipset, cids] =
        createNewTipset(10, SYNC_STATE_UNSYNCED, BigInt("100500"), 100, 500);

    insertUnsyncedTipset(*db, tipset, cids);

    EXPECT_OUTCOME_TRUE_2(roots, db->getRoots());
    EXPECT_EQ(roots.size(), 1);
    expectEqual(roots[0], tipset);

    EXPECT_OUTCOME_TRUE_2(heads, db->getHeads());

    // only 1 tipset i.e. root and head simultaneously
    EXPECT_EQ(heads.size(), 1);
    expectEqual(heads[0], roots[0]);

    db.reset();

    if (reopen) {
      EXPECT_OUTCOME_TRUE_2(db_reopened, createIndexDb(file_name));
      EXPECT_OUTCOME_TRUE_2(roots_reopened, db_reopened->getRoots());
      EXPECT_EQ(roots_reopened.size(), 1);
      expectEqual(roots_reopened[0], roots[0]);
    }
  }

  void testMinimalFlow() {
    auto db = createEmptyDb(kMemoryDbName);

    uint64_t branch_id = 1;
    uint64_t height = 0;
    BigInt weight("1");
    size_t n_blocks = 1;

    auto [head_tipset, head_cids] = createNewTipset(
        n_blocks, SYNC_STATE_UNSYNCED, weight, height, branch_id, height);

    insertUnsyncedTipset(*db, head_tipset, head_cids);

    auto [head_msg_cid, head_bls_msgs, head_secp_msgs] =
        createMessageCids(0, 0);

    makeBlockSynced(
        *db, head_cids[0], head_msg_cid, head_bls_msgs, head_secp_msgs);

    EXPECT_OUTCOME_TRUE_2(
        head_sync_state,
        db->updateTipsetSyncState(head_tipset.tipset, boost::none));

    EXPECT_EQ(head_sync_state, SYNC_STATE_SYNCED);

    ++height;
    weight += 100500;

    auto [new_tipset, new_cids] = createNewTipset(
        n_blocks, SYNC_STATE_UNSYNCED, weight, height, branch_id, height);

    insertUnsyncedTipset(*db, new_tipset, new_cids);

    auto [new_msg_cid, new_bls_msgs, new_secp_msgs] = createMessageCids(10, 5);

    makeBlockSynced(*db, new_cids[0], new_msg_cid, new_bls_msgs, new_secp_msgs);

    EXPECT_OUTCOME_TRUE_2(new_sync_state,
                          db->updateTipsetSyncState(
                              new_tipset.tipset, std::ref(head_tipset.tipset)));

    EXPECT_EQ(new_sync_state, SYNC_STATE_SYNCED);
  }

  void testStraightBranch(uint64_t max_height) {
    auto db = createEmptyDb(kMemoryDbName);

    uint64_t branch_id = 1;
    uint64_t height = 0;
    BigInt weight("1");
    size_t n_blocks = 1;

    auto [head_tipset, head_cids] = createNewTipset(
        n_blocks, SYNC_STATE_UNSYNCED, weight, height, branch_id, height);

    insertUnsyncedTipset(*db, head_tipset, head_cids);

    auto [head_msg_cid, head_bls_msgs, head_secp_msgs] =
        createMessageCids(0, 0);

    makeBlockSynced(
        *db, head_cids[0], head_msg_cid, head_bls_msgs, head_secp_msgs);

    EXPECT_OUTCOME_TRUE_2(
        head_sync_state,
        db->updateTipsetSyncState(head_tipset.tipset, boost::none));

    EXPECT_EQ(head_sync_state, SYNC_STATE_SYNCED);

    n_blocks = 2;

    for (height = 1; height <= max_height; ++height) {
      weight += 100500;

      auto [new_tipset, new_cids] = createNewTipset(
          n_blocks, SYNC_STATE_UNSYNCED, weight, height, branch_id, height);

      insertUnsyncedTipset(*db, new_tipset, new_cids);

      for (const auto& block_cid : new_cids) {
        auto [new_msg_cid, new_bls_msgs, new_secp_msgs] =
            createMessageCids(2, 2);

        makeBlockSynced(
            *db, block_cid, new_msg_cid, new_bls_msgs, new_secp_msgs);
      }

      EXPECT_OUTCOME_TRUE_2(
          new_sync_state,
          db->updateTipsetSyncState(new_tipset.tipset,
                                    std::ref(head_tipset.tipset)));

      EXPECT_EQ(new_sync_state, SYNC_STATE_SYNCED);
    }

    EXPECT_OUTCOME_TRUE_2(p, db->getBranchSyncState(branch_id));
    EXPECT_EQ(p.first, branch_id);
    EXPECT_EQ(p.second, SYNC_STATE_SYNCED);
  }

}  // namespace

TEST(IndexDb, CreationAndReopeningMemory) {
  try {
    testCreationReopening(kMemoryDbName, false);
  } catch (const std::exception &e) {
    EXPECT_EQ(std::string(e.what()), "unexpected");
  }
}

TEST(IndexDb, CreationAndReopeningFile) {
  std::string file_name("TestCreationAndReopening.db");
  Unlink u(file_name);
  testCreationReopening(file_name, true);
}

TEST(IndexDb, MinimalFlow2Tipsets) {
  testMinimalFlow();
}

TEST(IndexDb, StraightBranch) {
  testStraightBranch(1000);
}

/**
 * @given
 * @when
 * @then
 */
// TEST(IndexDb, Blobs_Graph) {
//  EXPECT_OUTCOME_TRUE_2(db, createIndexDb(":memory:"));
//
//  auto graph = createGraph<BlobGraph>({1, 2, 3, 4, 3, 2, 1});
//
//  insertGraph(*db, graph);
//
//  std::vector<IndexDb::Blob> blobs;
//  EXPECT_OUTCOME_TRUE_1(db->getParents(
//      graph[4][0], [&](const IndexDb::Blob &b) { blobs.push_back(b); }));
//
//  EXPECT_EQ(blobs.size(), 4);
//  for (auto &b : graph[3]) {
//    EXPECT_TRUE(contains(blobs, b));
//  }
//
//  const auto &root = graph[0][0];
//  auto from = graph.back()[0];
//  bool root_found = false;
//  for (auto i = 0u; i < graph.size(); ++i) {
//    blobs.clear();
//    EXPECT_OUTCOME_TRUE_1(db->getParents(from, [&](const IndexDb::Blob &b) {
//      if (blobs.empty()) {
//        blobs.push_back(b);
//      }
//      if (b == root) {
//        root_found = true;
//      }
//    }));
//    if (root_found) break;
//    from = blobs[0];
//  }
//  EXPECT_TRUE(root_found);
//}

/**
 * @given
 * @when
 * @then
 */
// TEST(IndexDb, CIDs_Graph) {
//  EXPECT_OUTCOME_TRUE_2(db, createIndexDb(":memory:"));
//
//  auto graph = createGraph<CIDGraph>({1, 2, 3, 4, 3, 2, 1});
//
//  insertGraph(*db, graph);
//
//  EXPECT_OUTCOME_TRUE_2(cids, getParents(*db, graph[4][0]));
//
//  EXPECT_EQ(cids.size(), 4);
//  for (auto &c : graph[3]) {
//    EXPECT_TRUE(contains(cids, c));
//  }
//
//  const auto &root = graph[0][0];
//  auto from = graph.back()[0];
//  bool root_found = false;
//  for (auto i = 0u; i < graph.size(); ++i) {
//    EXPECT_OUTCOME_TRUE_2(cc, getParents(*db, from));
//    if (cc.empty()) {
//      break;
//    }
//    if (contains(cc, root)) {
//      root_found = true;
//    }
//    if (root_found) break;
//    from = cc[0];
//  }
//  EXPECT_TRUE(root_found);
//}
