/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "sync_job.hpp"
#include "tipset_loader.hpp"

namespace fc::sync {

  SyncJob::SyncJob(libp2p::protocol::Scheduler &scheduler,
                   TipsetLoader &tipset_loader,
                   storage::indexdb::IndexDb &index_db,
                   Callback callback)
      : scheduler_(scheduler),
        tipset_loader_(tipset_loader),
        index_db_(index_db),
        callback_(std::move(callback)) {
    assert(callback_);
  }

  void SyncJob::start(PeerId peer, TipsetKey head, uint64_t probable_depth) {
    using namespace storage::indexdb;

    if (active_) {
      // log~~
      return;
    }
    active_ = true;

    status_.peer = std::move(peer);
    status_.head = std::move(head);

    try {
      if (!index_db_.tipsetExists(head.hash())) {
        // not indexed, loading...
        OUTCOME_EXCEPT(tipset_loader_.loadTipsetAsync(
            status_.head.value(), status_.peer, probable_depth));
        status_.next = status_.head.value().hash();
        status_.code = SyncStatus::IN_PROGRESS;
        return;
      }

      OUTCOME_EXCEPT(tipset_info, index_db_.getTipsetInfo(head.hash()));

      // we should resume interrupted syncing
      status_.branch = tipset_info.branch;

      bool already_synced = (status_.branch == kGenesisBranch);

      if (!already_synced) {
        OUTCOME_EXCEPT(branch_info, index_db_.getBranchInfo(status_.branch));

        if (branch_info.root == kGenesisBranch) {
          already_synced = true;
        } else {
          OUTCOME_EXCEPT(root_branch,
                         index_db_.getBranchInfo(branch_info.root));
          OUTCOME_EXCEPT(parent_key,
                         index_db_.getParentTipsetKey(root_branch.bottom));
          status_.last_loaded = root_branch.bottom;
          status_.next = parent_key.hash();
          OUTCOME_EXCEPT(tipset_loader_.loadTipsetAsync(
              parent_key, status_.peer, root_branch.bottom_height));
          status_.branch = root_branch.id;
          status_.code = SyncStatus::IN_PROGRESS;
        }
      }

      if (already_synced) {
        status_.code = SyncStatus::SYNCED_TO_GENESIS;
        scheduleCallback();
      }
    } catch (const std::system_error &e) {
      // log ~~~
      internalError(e.code());
    }
  }

  void SyncJob::cancel() {
    if (active_) {
      SyncStatus s;
      std::swap(s, status_);
      cb_handle_.cancel();
      active_ = false;
    }
  }

  bool SyncJob::isActive() const {
    return active_;
  }

  const SyncStatus &SyncJob::getStatus() const {
    return status_;
  }

  void SyncJob::onTipsetLoaded(TipsetHash hash,
                               outcome::result<Tipset> result) {
    if (status_.code != SyncStatus::IN_PROGRESS || !status_.next.has_value()
        || hash != status_.next.value()) {
      // dont need this tipset
      return;
    }

    try {
      OUTCOME_EXCEPT(tipset, result);
      OUTCOME_EXCEPT(parent_key, tipset.getParents());
      bool parent_exists = index_db_.tipsetExists(parent_key.hash());
      OUTCOME_EXCEPT(
          apply_result,
          index_db_.applyTipset(
              tipset, parent_exists, parent_key.hash(), status_.last_loaded));

      status_.last_loaded = tipset.key.hash();
      status_.branch = apply_result.this_branch;

      if (apply_result.root_is_genesis) {
        status_.next = parent_key.hash();
        status_.code = SyncStatus::SYNCED_TO_GENESIS;
        scheduleCallback();
        return;
      }

      if (parent_exists) {
        OUTCOME_EXCEPT(roots,
                       index_db_.getUnsyncedRootsOf(parent_key.hash()));
        status_.last_loaded = std::move(roots.last_loaded);
        status_.next = roots.to_load.hash();
        status_.branch = roots.branch;

        OUTCOME_EXCEPT(tipset_loader_.loadTipsetAsync(
            roots.to_load, status_.peer, roots.height));
      }

    } catch (const std::system_error &e) {
      // TODO (artem) separate bad blocks error vs. other errors
    }
  }

  void SyncJob::internalError(std::error_code e) {
    status_.error = e;
    status_.code = SyncStatus::INTERNAL_ERROR;
    scheduleCallback();
  }

  void SyncJob::scheduleCallback() {
    cb_handle_ = scheduler_.schedule([this]() {
      SyncStatus s;
      std::swap(s, status_);
      active_ = false;
      callback_(s);
    });
  }

  void Syncer::start() {
    if (!started_) {
      started_ = true;
    }

    if (!isActive()) {
      auto target = chooseNextTarget();
      if (target) {
        auto &t = target.value()->second;
        startJob(target.value()->first, std::move(t.head_tipset), t.height);
        pending_targets_.erase(target.value());
      }
    }
  }

  void Syncer::newTarget(PeerId peer,
                         TipsetKey head_tipset,
                         BigInt weight,
                         uint64_t height) {
    if (weight <= current_weight_) {
      // not a sync target
      return;
    }

    if (started_ && !isActive()) {
      startJob(std::move(peer), std::move(head_tipset), height);
    } else {
      pending_targets_[peer] =
          Target{std::move(head_tipset), std::move(weight), std::move(height)};
    }
  }

  void Syncer::excludePeer(const PeerId &peer) {
    pending_targets_.erase(peer);
  }

  void Syncer::setCurrentWeightAndHeight(BigInt w, uint64_t h) {
    current_weight_ = std::move(w);
    current_height_ = h;

    for (auto it = pending_targets_.begin(); it != pending_targets_.end();) {
      if (it->second.weight <= current_weight_) {
        it = pending_targets_.erase(it);
      } else {
        ++it;
      }
    }
  }

  bool Syncer::isActive() {
    return started_ && current_job_ && current_job_->isActive();
  }

  boost::optional<Syncer::PendingTargets::iterator> Syncer::chooseNextTarget() {
    boost::optional<PendingTargets::iterator> target;
    if (!pending_targets_.empty()) {
      BigInt max_weight = current_weight_;
      for (auto it = pending_targets_.begin(); it != pending_targets_.end();
           ++it) {
        if (it->second.weight > max_weight) {
          max_weight = it->second.weight;
          target = it;
        }
      }
      if (!target) {
        // all targets are obsolete, forget them
        pending_targets_.clear();
      }
    }

    // TODO (artem) choose peer by minimal latency among connected peers with
    // the same weight. Using PeerManager

    return target;
  }

  void Syncer::startJob(PeerId peer, TipsetKey head_tipset, uint64_t height) {
    assert(started_);

    if (!current_job_) {
      current_job_ = std::make_unique<SyncJob>(
          *scheduler_, *tipset_loader_, *index_db_, [this](SyncStatus status) {
            onSyncJobFinished(std::move(status));
          });
    }

    assert(!current_job_->isActive());

    uint64_t probable_depth = height;
    if (height > current_height_) {
      probable_depth = height - current_height_;
    }

    current_job_->start(
        std::move(peer), std::move(head_tipset), probable_depth);
  }

  void Syncer::onTipsetLoaded(TipsetHash hash, outcome::result<Tipset> tipset) {
    if (isActive()) {
      current_job_->onTipsetLoaded(std::move(hash), std::move(tipset));
    }
  }

  void Syncer::onSyncJobFinished(SyncStatus status) {
    callback_(std::move(status));
  }

}  // namespace fc::sync