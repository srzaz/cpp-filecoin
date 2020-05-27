/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "tipset_hash.hpp"

#include "crypto/blake2/blake2b160.hpp"

namespace fc::primitives::tipset {

  static TipsetHash tipsetHash(std::vector<uint8_t> tipset_bytes) {
    auto hash_array = crypto::blake2b::blake2b_256(tipset_bytes);
    return TipsetHash(hash_array.data(), hash_array.data() + hash_array.size());
  }

  TipsetHash tipsetHash(const TipsetKey &key) {
    return key.bytes.empty() ? tipsetHash(key.bytes) : tipsetHash(key.cids);
  }

  TipsetHash tipsetHash(const Tipset &tipset) {
    return tipsetHash(tipset.makeKey().value());
  }

}  // namespace fc::primitives::tipset
