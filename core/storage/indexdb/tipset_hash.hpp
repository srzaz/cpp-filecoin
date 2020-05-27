/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef CPP_FILECOIN_TIPSET_HASH_HPP
#define CPP_FILECOIN_TIPSET_HASH_HPP

#include "primitives/tipset/tipset.hpp"

namespace fc::primitives::tipset {

  using TipsetHash = std::vector<uint8_t>;

  TipsetHash tipsetHash(const TipsetKey& key);
  TipsetHash tipsetHash(const Tipset& tipset);
  TipsetHash tipsetHash(const std::vector<CID>& cids);

}

#endif  // CPP_FILECOIN_TIPSET_HASH_HPP
