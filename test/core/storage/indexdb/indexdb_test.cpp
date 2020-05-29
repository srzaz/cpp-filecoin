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

  std::pair<TipsetInfo, std::vector<fc::CID>> createNewTipset(
      size_t n_blocks,
      SyncState sync_state,
      BigInt weight,
      uint64_t height,
      uint64_t branch_id,
      size_t blocks_unique_prefix = 0) {
    std::pair<TipsetInfo, std::vector<fc::CID>> ret;
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

    EXPECT_OUTCOME_TRUE_1(db->newTipset(
        tipset.tipset, cids, tipset.weight, tipset.height, tipset.branch_id));

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

}  // namespace

TEST(IndexDb, CreationAndReopeningMemory) {
  try {
    testCreationReopening(":memory:", false);
  } catch (const std::exception &e) {
    EXPECT_EQ(std::string(e.what()), "unexpected");
  }
}

TEST(IndexDb, CreationAndReopeningFile) {
  std::string file_name("TestCreationAndReopening.db");
  Unlink u(file_name);
  testCreationReopening(file_name, true);
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
