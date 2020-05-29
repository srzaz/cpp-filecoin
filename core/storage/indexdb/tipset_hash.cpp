/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "tipset_hash.hpp"

#include "crypto/blake2/blake2b.h"
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

  TipsetHash tipsetHash(const std::vector<CID> &cids) {
    blake2b_ctx ctx;

    if (blake2b_init(&ctx, crypto::blake2b::BLAKE2B256_HASH_LENGTH, nullptr, 0)
        != 0) {
      return {};
    }

    for (const auto& cid : cids) {
      auto res = cid.toBytes();
      if (res) {
        blake2b_update(&ctx, res.value().data(), res.value().size());
      }
    }

    TipsetHash hash;
    hash.resize(crypto::blake2b::BLAKE2B256_HASH_LENGTH);

    blake2b_final(&ctx, hash.data());
    return hash;
  }

}  // namespace fc::primitives::tipset
