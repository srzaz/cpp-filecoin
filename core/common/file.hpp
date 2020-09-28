/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <boost/optional.hpp>

#include "common/buffer.hpp"

namespace fc::common {
  boost::optional<Buffer> readFile(std::string_view path);
}  // namespace fc::common
