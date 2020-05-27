/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef CPP_FILECOIN_SYNC_DOWNLOADER_HPP
#define CPP_FILECOIN_SYNC_DOWNLOADER_HPP

#include <unordered_set>
#include <set>

#include <boost/signals2.hpp>

#include <libp2p/protocol/common/scheduler.hpp>
#include <libp2p/protocol/gossip/gossip.hpp>

#include "storage/indexdb/tipset_hash.hpp"
#include "storage/indexdb/indexdb.hpp"
#include "storage/ipfs/datastore.hpp"
#include "storage/ipfs/graphsync/graphsync.hpp"

namespace fc::sync {

  enum class Error {
    SYNC_WRONG_OBJECT_TYPE = 1,
    SYNC_DATA_INTEGRITY_ERROR = 2,
    SYNC_UNEXPECTED_OBJECT_STATE = 3,
    SYNC_NO_PEERS = 4,
  };

  using crypto::signature::Signature;
  using primitives::block::Block;
  using primitives::block::BlockHeader;
  using primitives::block::BlockMsg;
  using primitives::tipset::Tipset;
  using primitives::tipset::TipsetHash;
  using vm::message::UnsignedMessage;
  using PeerId = libp2p::peer::PeerId;

  class Downloader {
   public:
    using connection_t = boost::signals2::connection;

    enum LoadState { load_success, object_is_invalid };

    struct ObjectAvailable {
      enum Source { from_local_db, from_pubsub, from_graphsync };

      const CID &cid;
      const common::Buffer &raw_data;
      //const PeerId &from;
      Source source;
      LoadState load_state;
    };

    // Any object downloaded by CID and available in local storages
    using OnObjectAvailable = void(const ObjectAvailable &event);

    connection_t subscribeToObjects(
        const std::function<OnObjectAvailable> &subscriber);

    struct MessageAvailable : ObjectAvailable {
      const UnsignedMessage &message;
      boost::optional<const Signature &> signature;
    };

    // Message downloaded and available in local storages
    using OnMessageAvailable = void(const MessageAvailable &event);

    connection_t subscribeToMessages(
        const std::function<OnMessageAvailable> &subscriber);

    struct BlockHeaderAvailable : ObjectAvailable {
      const BlockMsg &block_msg;
    };

    // Block header downloaded and available in local storages
    using OnBlockHeaderAvailable = void(const BlockHeaderAvailable &event);

    connection_t subscribeToBlockHeaders(
        const std::function<OnBlockHeaderAvailable> &subscriber);

    struct FullBlockAvailable : ObjectAvailable {
      const BlockHeader &block;
    };

    // Block and all it objects downloaded and available in local storages
    using OnFullBlockAvailable = void(const FullBlockAvailable &event);

    connection_t subscribeToFullBlocks(
        const std::function<OnFullBlockAvailable> &subscriber);

    struct TipsetAvailable {
      LoadState load_state;
      const std::vector<CID> &cids;
      const TipsetHash &hash;
      uint64_t branch_id;
      boost::optional<const Tipset &> tipset;
    };

    // Block and all it objects downloaded and available in local storages
    using OnTipsetAvailable = void(const TipsetAvailable &event);

    connection_t subscribeToTipsets(
        const std::function<OnTipsetAvailable> &subscriber);

    void setPreferredPeer(PeerId peer_id);

    enum ExpectedObjectType {
      any_object_expected,
      message_expected,
      block_expected
    };

    outcome::result<void> loadObjectByCid(
        CID cid,
        boost::optional<PeerId> from,
        ExpectedObjectType what_to_expect = any_object_expected);

    outcome::result<void> loadTipset(std::vector<CID> cids, uint64_t branch_id);

    Downloader(std::shared_ptr<storage::indexdb::IndexDb> index_db,
               std::shared_ptr<storage::ipfs::IpfsDatastore> ipfs_db,
               std::shared_ptr<storage::ipfs::graphsync::Graphsync> graphsync,
               std::shared_ptr<libp2p::protocol::gossip::Gossip> gossip,
               std::shared_ptr<libp2p::protocol::Scheduler> scheduler,
               PeerId local_peer_id);

   private:
    struct CIDRequest {
      libp2p::protocol::Subscription subscription;
      //PeerId peer;
      ExpectedObjectType what_to_expect = any_object_expected;
    };

    using Requests = std::unordered_map<CID, std::shared_ptr<CIDRequest>>;

    //using Wantlist = std::unordered_set<CID>;
    using Wantlist = std::set<CID>;

    struct BlockCrafter {
      Downloader &owner;

      CID block_cid;

      bool header_wanted = true;
      Wantlist msgs_wantlist;

      boost::optional<BlockMsg> block_msg;

      libp2p::protocol::scheduler::Handle call_completed_;

      BlockCrafter(Downloader &o, CID c);

      void onHeaderSynced(const BlockMsg &msg);

      void onMessageSynced(const CID &msg_cid);

      void sayCompleted();
    };

    struct TipsetCrafter {
      Downloader &owner;

      // cids of blocks needed
      std::vector<CID> cids;

      TipsetHash hash;

      uint64_t branch_id;

      // block cids we are waiting for
      Wantlist wantlist;

      // the puzzle being filled
      std::vector<boost::optional<BlockHeader>> blocks_filled;

      libp2p::protocol::scheduler::Handle call_completed_;

      TipsetCrafter(Downloader &o,
                    std::vector<CID> c,
                    TipsetHash h,
                    uint64_t branch);

      void onBlockSynced(const CID &cid, const BlockHeader &bh);
    };

    void onBlockCompleted(const CID &block_cid, const BlockMsg &msg);

    void onTipsetCompleted(outcome::result<Tipset> tipset,
                           const std::vector<CID> &cids,
                           const TipsetHash &hash,
                           uint64_t branch_id);

    outcome::result<void> loadBlock(
        const CID &cid, storage::indexdb::SyncState current_state);

    outcome::result<void> loadMessages(const std::vector<CID> &cids);

    storage::ipfs::graphsync::Graphsync::RequestProgressCallback
    makeRequestProgressCallback(CID cid);

    void onGraphsyncResponseProgress(
        const CID &cid, storage::ipfs::graphsync::ResponseStatusCode status);

    void onGraphsyncData(const CID &cid, const common::Buffer &data);

    std::shared_ptr<storage::indexdb::IndexDb> index_db_;
    std::shared_ptr<storage::ipfs::IpfsDatastore> ipfs_db_;
    std::shared_ptr<storage::ipfs::graphsync::Graphsync> graphsync_;
    std::shared_ptr<libp2p::protocol::gossip::Gossip> gossip_;
    std::shared_ptr<libp2p::protocol::Scheduler> scheduler_;

    const PeerId local_peer_id_;
    boost::optional<PeerId> preferred_peer_;
    boost::optional<PeerId> last_received_from_;
    ObjectAvailable::Source source_from_ = ObjectAvailable::from_local_db;

    boost::signals2::signal<OnObjectAvailable> objects_signal_;
    boost::signals2::signal<OnMessageAvailable> messages_signal_;
    boost::signals2::signal<OnBlockHeaderAvailable> headers_signal_;
    boost::signals2::signal<OnFullBlockAvailable> blocks_signal_;
    boost::signals2::signal<OnTipsetAvailable> tipsets_signal_;

    std::map<TipsetHash, TipsetCrafter> tipset_requests_;
    std::unordered_map<CID, BlockCrafter> block_requests_;
    Requests cid_requests_;
    std::unordered_set<CID> need_to_repeat_request_;

    // friendship with contained objects is ok
    friend TipsetCrafter;
    friend BlockCrafter;
  };

}  // namespace fc::sync

OUTCOME_HPP_DECLARE_ERROR(fc::sync, Error);

#endif  // CPP_FILECOIN_SYNC_DOWNLOADER_HPP
