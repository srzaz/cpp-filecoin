/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef CPP_FILECOIN_SYNC_BLOCKSYNC_SERVER_HPP
#define CPP_FILECOIN_SYNC_BLOCKSYNC_SERVER_HPP

#include "sync/blocksync_common.hpp"

namespace libp2p {
  class Host;
}

namespace fc::sync::blocksync {
  void serveBlocksync(std::shared_ptr<libp2p::Host> host, IpldPtr ipld);
}  // namespace fc::sync::blocksync

#endif  // CPP_FILECOIN_SYNC_BLOCKSYNC_SERVER_HPP
