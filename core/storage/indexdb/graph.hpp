/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef CPP_FILECOIN_STORAGE_INDEXDB_GRAPH_HPP
#define CPP_FILECOIN_STORAGE_INDEXDB_GRAPH_HPP

#include <map>
#include <set>

#include "primitives/tipset/tipset_key.hpp"

namespace fc::storage::indexdb {

  using BranchId = uint64_t;
  using Height = uint64_t;
  using TipsetHash = primitives::tipset::TipsetHash;

  constexpr BranchId kZeroBranchId = 0;

  /// Graph of chain branches: auxiliary structure used by IndexDB
  class Graph {
   public:
    enum class Error {
      GRAPH_LOAD_ERROR = 1,
      NO_CURRENT_CHAIN,
      BRANCH_NOT_FOUND,
      BRANCH_IS_NOT_A_HEAD,
      CYCLE_DETECTED,
      BRANCH_IS_NOT_A_ROOT,
      LINK_HEIGHT_MISMATCH,
    };

    struct Branch {
      BranchId id = kZeroBranchId;
      TipsetHash top;
      Height top_height = 0;
      TipsetHash bottom;
      Height bottom_height = 0;
      BranchId parent = kZeroBranchId;
      std::set<BranchId> forks;
    };

    using Branches = std::vector<std::reference_wrapper<const Branch>>;

    Branches getRoots() const;

    Branches getHeads() const;

    BranchId getLastBranchId() const;

    /// Finds branch by height in current chain
    outcome::result<BranchId> findByHeight(Height height) const;

    outcome::result<void> load(std::vector<Branch> all_branches);

    outcome::result<void> switchToHead(BranchId head);

    /// All removes can be performed from head only
    /// returns parent branch id and successor branch id
    outcome::result<std::pair<BranchId, BranchId>> removeHead(BranchId head);

    /// Returns branch id of the new base
    outcome::result<BranchId> linkBranches(BranchId base_branch,
                                           BranchId successor_branch,
                                           TipsetHash base_tipset,
                                           Height base_height);

    outcome::result<void> linkToHead(BranchId base_branch,
                                     BranchId successor_branch);

    void clear();

   private:
    Branches getRootsOrHeads(const std::set<BranchId> &ids) const;

    outcome::result<void> loadFailed();

    outcome::result<std::pair<BranchId, BranchId>> merge(Branch &b);

    std::map<BranchId, Branch> all_branches_;
    std::set<BranchId> roots_;
    std::set<BranchId> heads_;
    std::map<Height, BranchId> current_chain_;
    Height current_chain_bottom_height_ = 0;
  };

}  // namespace fc::storage::indexdb

OUTCOME_HPP_DECLARE_ERROR(fc::storage::indexdb, Graph::Error);

#endif  // CPP_FILECOIN_STORAGE_INDEXDB_GRAPH_HPP
