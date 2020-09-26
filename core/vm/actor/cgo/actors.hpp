/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "node/fwd.hpp"
#include "primitives/sector/sector.hpp"

namespace fc::vm::actor::cgo {
  using message::UnsignedMessage;
  using primitives::BigInt;
  using primitives::FilterEstimate;
  using primitives::StoragePower;
  using primitives::sector::RegisteredProof;
  using runtime::Execution;

  void config(const StoragePower &min_verified_deal_size,
              const StoragePower &consensus_miner_min_power,
              const std::vector<RegisteredProof> &supported_proofs);

  outcome::result<Buffer> invoke(const std::shared_ptr<Execution> &exec,
                                 const UnsignedMessage &message,
                                 const CID &code,
                                 size_t method,
                                 BytesIn params);

  BigInt initialPledgeForPower(const BigInt &,
                               const BigInt &,
                               const BigInt &,
                               const FilterEstimate &,
                               const FilterEstimate &,
                               const BigInt &);
}  // namespace fc::vm::actor::cgo
