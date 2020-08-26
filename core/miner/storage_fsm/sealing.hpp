/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef CPP_FILECOIN_CORE_MINER_STORAGE_FSM_SEALING_HPP
#define CPP_FILECOIN_CORE_MINER_STORAGE_FSM_SEALING_HPP

#include "common/outcome.hpp"
#include "miner/storage_fsm/sealing_states.hpp"
#include "primitives/address/address.hpp"
#include "primitives/piece/piece.hpp"
#include "primitives/piece/piece_data.hpp"
#include "primitives/sector/sector.hpp"
#include "primitives/tipset/tipset_key.hpp"
#include "primitives/types.hpp"
#include "sector_storage/manager.hpp"

namespace fc::mining {
  using primitives::ChainEpoch;
  using primitives::DealId;
  using primitives::SectorNumber;
  using primitives::address::Address;
  using primitives::piece::PieceData;
  using primitives::piece::PieceInfo;
  using primitives::piece::UnpaddedPieceSize;
  using primitives::tipset::TipsetKey;
  using proofs::SealRandomness;
  using sector_storage::InteractiveRandomness;
  using sector_storage::PreCommit1Output;

  struct SectorInfo {
    primitives::SectorNumber sector_number;
    std::vector<PieceInfo> pieces;

    SealRandomness ticket;
    ChainEpoch ticket_epoch;
    PreCommit1Output precommit1_output;
    uint64_t precommit2_fails;

    CID comm_d;
    CID comm_r;

    boost::optional<CID> precommit_message;

    TipsetKey precommit_tipset;

    InteractiveRandomness seed;
    ChainEpoch seed_epoch;

    proofs::Proof proof;
    boost::optional<CID> message;
    uint64_t invalid_proofs;

    inline std::vector<UnpaddedPieceSize> existingPieceSizes() const {
      std::vector<UnpaddedPieceSize> result;

      for (const auto &piece : pieces) {
        result.push_back(piece.size.unpadded());
      }

      return result;
    }
  };

  /**
   * DealSchedule communicates the time interval of a storage deal. The deal
   * must appear in a sealed (proven) sector no later than StartEpoch, otherwise
   * it is invalid.
   */
  struct DealSchedule {
    ChainEpoch start_epoch;
    ChainEpoch end_epoch;
  };

  /** DealInfo is a tuple of deal identity and its schedule */
  struct DealInfo {
    DealId deal_id;
    DealSchedule deal_schedule;
  };

  // Epochs
  constexpr int kInteractivePoRepConfidence = 6;

  class Sealing {
   public:
    virtual ~Sealing() = default;

    virtual outcome::result<void> run() = 0;

    virtual void stop() = 0;

    virtual outcome::result<SectorNumber> allocatePiece(
        UnpaddedPieceSize size) = 0;

    virtual outcome::result<void> sealPiece(UnpaddedPieceSize size,
                                            const PieceData &piece_data,
                                            SectorNumber sector_id,
                                            DealInfo deal) = 0;

    virtual outcome::result<void> remove(SectorNumber sector_id) = 0;

    virtual Address getAddress() const = 0;

    virtual std::unordered_map<std::shared_ptr<SectorInfo>, SealingState>
    getListSectors() const = 0;

    virtual outcome::result<std::shared_ptr<SectorInfo>> getSectorInfo(
        SectorNumber id) const = 0;

    virtual outcome::result<void> forceSectorState(SectorNumber id,
                                                   SealingState state) = 0;
  };
}  // namespace fc::mining

#endif  // CPP_FILECOIN_CORE_MINER_STORAGE_FSM_SEALING_HPP