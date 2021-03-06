/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "vm/interpreter/interpreter.hpp"

namespace fc::blockchain::production {
  using primitives::block::BlockTemplate;
  using primitives::block::BlockWithMessages;
  using vm::interpreter::Interpreter;

  constexpr size_t kBlockMaxMessagesCount = 1000;

  outcome::result<BlockWithMessages> generate(Interpreter &interpreter,
                                              std::shared_ptr<Ipld> ipld,
                                              BlockTemplate t);
}  // namespace fc::blockchain::production
