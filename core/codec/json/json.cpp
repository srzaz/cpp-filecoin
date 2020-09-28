/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "codec/json/json.hpp"

#include <rapidjson/writer.h>
#include <cppcodec/base64_rfc4648.hpp>

namespace fc::codec::json {
  using base64 = cppcodec::base64_rfc4648;

  boost::optional<Document> parse(BytesIn input) {
    Document doc;
    doc.Parse((const char *)input.data(), input.size());
    if (doc.HasParseError()) {
      return {};
    }
    return doc;
  }

  Opt get(const Opt &j, std::string_view key) {
    if (j && j->IsObject()) {
      auto it{j->FindMember(key.data())};
      if (it != j->MemberEnd()) {
        return &it->value;
      }
    }
    return {};
  }

  boost::optional<std::string_view> str(const Opt &j) {
    if (j && j->IsString()) {
      return std::string_view{j->GetString(), j->GetStringLength()};
    }
    return {};
  }

  boost::optional<Buffer> bytes(OptIn j) {
    if (auto s{str(j)}) {
      return Buffer{base64::decode(*s)};
    }
    return {};
  }

  boost::optional<CID> cid(OptIn j) {
    if (auto o{get(j, "/")}) {
      if (auto s{str(o)}) {
        OUTCOME_EXCEPT(cid, CID::fromString(std::string{*s}));
        return cid;
      }
    }
    return {};
  }

  boost::optional<int64_t> Int(OptIn j) {
    if (j && j->IsInt64()) {
      return j->GetInt64();
    }
    return {};
  }
}  // namespace fc::codec::json
