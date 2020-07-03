/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "blocksync_server.hpp"

#include <libp2p/host/host.hpp>

#include "common/libp2p/cbor_stream.hpp"

namespace fc::sync::blocksync {
  template <typename T>
  struct MessageVisitor {
    outcome::result<void> operator()(size_t, const CID &cid) {
      auto index{indices.find(cid)};
      if (index == indices.end()) {
        index = indices.emplace(cid, messages.size()).first;
        OUTCOME_TRY(message, ipld->getCbor<T>(cid));
        messages.push_back(std::move(message));
      }
      includes.rbegin()->push_back(index->second);
      return outcome::success();
    }

    IpldPtr &ipld;
    std::vector<T> &messages;
    MsgIncudes &includes;
    std::map<CID, size_t> indices{};
  };

  outcome::result<std::vector<TipsetBundle>> getChain(IpldPtr ipld,
                                                      const Request &request) {
    OUTCOME_TRY(ts, Tipset::load(*ipld, request.block_cids));
    std::vector<TipsetBundle> chain;
    while (true) {
      TipsetBundle bundle;
      if (request.options & RequestOptions::MESSAGES) {
        MessageVisitor<vm::message::UnsignedMessage> bls_visitor{
            ipld, bundle.bls_msgs, bundle.bls_msg_includes};
        MessageVisitor<vm::message::SignedMessage> secp_visitor{
            ipld, bundle.secp_msgs, bundle.secp_msg_includes};
        for (auto &block : ts.blks) {
          OUTCOME_TRY(meta, ipld->getCbor<MsgMeta>(block.messages));
          bundle.bls_msg_includes.emplace_back();
          OUTCOME_TRY(meta.bls_messages.visit(bls_visitor));
          bundle.secp_msg_includes.emplace_back();
          OUTCOME_TRY(meta.secp_messages.visit(secp_visitor));
        }
      }
      if (request.options & RequestOptions::BLOCKS) {
        bundle.blocks = ts.blks;
      }
      if (chain.size() >= request.depth || ts.height() == 0) {
        break;
      }
      OUTCOME_TRYA(ts, ts.loadParent(*ipld));
    }
    return chain;
  }

  void serveBlocksync(std::shared_ptr<libp2p::Host> host, IpldPtr ipld) {
    host->setProtocolHandler(protocol_id, [ipld](auto rstream) {
      auto stream{std::make_shared<common::libp2p::CborStream>(rstream)};
      stream->template read<Request>([stream, ipld](auto _request) {
        Response response;
        if (_request) {
          auto &&request{_request.value()};
          if (request.block_cids.empty()) {
            response.status = ResponseStatus::BAD_REQUEST;
            response.message = "no cids given in blocksync request";
          } else {
            auto partial{request.depth > kBlockSyncMaxRequestLength};
            if (partial) {
              request.depth = kBlockSyncMaxRequestLength;
            }
            auto _chain{getChain(ipld, request)};
            if (_chain) {
              response.chain = std::move(_chain.value());
              response.status = partial ? ResponseStatus::RESPONSE_PARTIAL
                                        : ResponseStatus::RESPONSE_COMPLETE;
            } else {
              response.status = ResponseStatus::INTERNAL_ERROR;
              response.message = _chain.error().message();
            }
          }
        } else {
          response.status = ResponseStatus::BAD_REQUEST;
          response.message = _request.error().message();
        }
        stream->write(response, [stream](auto _n) {
          std::ignore = _n;
          stream->stream()->close([](auto _close) { std::ignore = _close; });
        });
      });
    });
  }
}  // namespace fc::sync::blocksync
