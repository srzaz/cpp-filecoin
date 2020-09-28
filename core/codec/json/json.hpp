/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <rapidjson/document.h>
#include <boost/optional.hpp>

#include "common/buffer.hpp"
#include "primitives/cid/cid.hpp"

namespace fc::codec::json {
  using rapidjson::Document;
  using rapidjson::Value;
  using Opt = const Value *;
  using OptIn = const Opt &;

  boost::optional<Document> parse(BytesIn input);

  Opt get(OptIn j, std::string_view key);

  boost::optional<std::string_view> str(OptIn j);

  boost::optional<Buffer> bytes(OptIn j);

  boost::optional<CID> cid(OptIn j);

  template <typename F>
  inline auto list(OptIn j, const F &f) {
    using T = std::invoke_result_t<F, OptIn>;
    boost::optional<std::vector<T>> list;
    if (j && j->IsArray()) {
      list.emplace();
      list->reserve(j->Size());
      for (auto it{j->Begin()}; it != j->End(); ++it) {
        list->push_back(f(&*it));
      }
    }
    return list;
  }

  boost::optional<int64_t> Int(OptIn j);
}  // namespace fc::codec::json
