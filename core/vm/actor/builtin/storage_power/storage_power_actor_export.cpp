/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "vm/actor/builtin/storage_power/storage_power_actor_export.hpp"

#include "common/logger.hpp"
#include "vm/actor/builtin/init/init_actor.hpp"
#include "vm/actor/builtin/miner/miner_actor.hpp"
#include "vm/actor/builtin/reward/reward_actor.hpp"
#include "vm/actor/builtin/shared/shared.hpp"
#include "vm/actor/builtin/storage_power/storage_power_actor_state.hpp"

namespace fc::vm::actor::builtin::storage_power {
  using primitives::SectorNumber;

  outcome::result<TokenAmount> computeInitialPledge(
      Runtime &runtime, State &state, const SectorStorageWeightDesc &weight) {
    OUTCOME_TRY(
        epoch_reward,
        runtime.sendM<reward::LastPerEpochReward>(kRewardAddress, {}, 0));
    TokenAmount circ_supply;  // unused yet, TODO: runtime
    return initialPledgeForWeight(qaPowerForWeight(weight),
                                  state.total_qa_power,
                                  circ_supply,
                                  state.total_pledge,
                                  epoch_reward);
  }

  outcome::result<void> processDeferredCronEvents(Runtime &runtime) {
    OUTCOME_TRY(state, runtime.getCurrentActorStateCbor<State>());
    auto now{runtime.getCurrentEpoch()};
    std::vector<CronEvent> pending;
    for (auto epoch = state.last_epoch_tick + 1; epoch <= now; ++epoch) {
      OUTCOME_TRY(events, state.cron_event_queue.tryGet(epoch));
      if (events) {
        OUTCOME_TRY(events->visit([&](auto, auto &event) {
          pending.push_back(event);
          return outcome::success();
        }));
        OUTCOME_TRY(state.cron_event_queue.remove(epoch));
      }
    }
    state.last_epoch_tick = now;
    OUTCOME_TRY(runtime.commitState(state));

    for (auto &event : pending) {
      auto res{runtime.send(event.miner_address,
                            miner::OnDeferredCronEvent::Number,
                            MethodParams{event.callback_payload},
                            0)};
      if (!res) {
        spdlog::warn(
            "PowerActor.processDeferredCronEvents: error {} \"{}\", epoch "
            "{}, miner {}, payload {}",
            res.error(),
            res.error().message(),
            now,
            event.miner_address,
            common::hex_lower(event.callback_payload));
      }
    }
    return outcome::success();
  }

  outcome::result<void> processBatchProofVerifies(Runtime &runtime) {
    OUTCOME_TRY(state, runtime.getCurrentActorStateCbor<State>());
    Runtime::MinerSeals miner_seals;
    if (state.proof_validation_batch) {
      OUTCOME_TRY(state.proof_validation_batch->visit(
          [&](auto &miner, auto array) -> outcome::result<void> {
            OUTCOME_TRY(seals, array.values());
            miner_seals.emplace_back(miner, std::move(seals));
            return outcome::success();
          }));
      state.proof_validation_batch.reset();
    }
    OUTCOME_TRY(runtime.commitState(state));

    OUTCOME_TRY(miner_bools, runtime.batchVerifySeals(miner_seals));
    auto _bools{miner_bools.begin()};
    for (auto &[miner, seals] : miner_seals) {
      miner::ConfirmSectorProofsValid::Params params;
      std::set<SectorNumber> sectors;
      auto seal{seals.begin()};
      for (auto verified : _bools->second) {
        if (verified && sectors.insert(seal->sector.sector).second) {
          params.sectors.push_back(seal->sector.sector);
        }
        ++seal;
      }
      OUTCOME_TRY(
          runtime.sendM<miner::ConfirmSectorProofsValid>(miner, params, 0));
      ++_bools;
    }
    return outcome::success();
  }

  outcome::result<void> deleteMinerActor(State &state, const Address &miner) {
    OUTCOME_TRY(state.claims.remove(miner));
    --state.miner_count;
    return outcome::success();
  }

  std::pair<StoragePower, StoragePower> powersForWeights(
      const std::vector<SectorStorageWeightDesc> &weights) {
    StoragePower raw{}, qa{};
    for (auto &w : weights) {
      raw += w.sector_size;
      qa += qaPowerForWeight(w);
    }
    return {raw, qa};
  }

  outcome::result<None> addToClaim(
      Runtime &runtime,
      bool add,
      const std::vector<SectorStorageWeightDesc> &weights) {
    OUTCOME_TRY(runtime.validateImmediateCallerIsMiner());
    OUTCOME_TRY(state, runtime.getCurrentActorStateCbor<State>());
    auto [raw, qa] = powersForWeights(weights);
    OUTCOME_TRY(state.addToClaim(
        runtime.getImmediateCaller(), add ? raw : -raw, add ? qa : -qa));
    OUTCOME_TRY(runtime.commitState(state));
    return outcome::success();
  }

  ACTOR_METHOD_IMPL(Construct) {
    OUTCOME_TRY(runtime.validateImmediateCallerIs(kSystemActorAddress));
    OUTCOME_TRY(runtime.commitState(State::empty(runtime)));
    return outcome::success();
  }

  ACTOR_METHOD_IMPL(CreateMiner) {
    OUTCOME_TRY(runtime.validateImmediateCallerIsSignable());
    OUTCOME_TRY(
        miner_params,
        encodeActorParams(miner::Construct::Params{params.owner,
                                                   params.worker,
                                                   params.seal_proof_type,
                                                   params.peer_id,
                                                   params.addresses}));
    OUTCOME_TRY(addresses_created,
                runtime.sendM<init::Exec>(kInitAddress,
                                          {kStorageMinerCodeCid, miner_params},
                                          runtime.getValueReceived()));
    OUTCOME_TRY(state, runtime.getCurrentActorStateCbor<State>());
    OUTCOME_TRY(state.claims.set(addresses_created.id_address, {0, 0}));
    ++state.miner_count;
    OUTCOME_TRY(runtime.commitState(state));
    return Result{addresses_created.id_address,
                  addresses_created.robust_address};
  }

  ACTOR_METHOD_IMPL(DeleteMiner) {
    OUTCOME_TRY(nominal, runtime.resolveAddress(params.miner));
    OUTCOME_TRY(miner, requestMinerControlAddress(runtime, nominal));
    if (runtime.getImmediateCaller() != miner.worker
        && runtime.getImmediateCaller() != miner.owner) {
      return VMExitCode::kSysErrForbidden;
    }
    OUTCOME_TRY(state, runtime.getCurrentActorStateCbor<State>());
    OUTCOME_TRY(claim, state.claims.get(nominal));
    VM_ASSERT(claim.raw_power >= 0);
    VM_ASSERT(claim.qa_power >= 0);
    state.total_raw_power -= claim.raw_power;
    state.total_qa_power -= claim.qa_power;
    OUTCOME_TRY(deleteMinerActor(state, nominal));
    OUTCOME_TRY(runtime.commitState(state));
    return outcome::success();
  }

  ACTOR_METHOD_IMPL(OnSectorProveCommit) {
    OUTCOME_TRY(runtime.validateImmediateCallerIsMiner());
    OUTCOME_TRY(state, runtime.getCurrentActorStateCbor<State>());
    OUTCOME_TRY(pledge, computeInitialPledge(runtime, state, params.weight));
    OUTCOME_TRY(state.addToClaim(runtime.getImmediateCaller(),
                                 params.weight.sector_size,
                                 qaPowerForWeight(params.weight)));
    OUTCOME_TRY(runtime.commitState(state));
    return std::move(pledge);
  }

  ACTOR_METHOD_IMPL(OnSectorTerminate) {
    return addToClaim(runtime, false, params.weights);
  }

  ACTOR_METHOD_IMPL(OnFaultBegin) {
    return addToClaim(runtime, false, params.weights);
  }

  ACTOR_METHOD_IMPL(OnFaultEnd) {
    return addToClaim(runtime, true, params.weights);
  }

  ACTOR_METHOD_IMPL(OnSectorModifyWeightDesc) {
    OUTCOME_TRY(runtime.validateImmediateCallerIsMiner());
    auto miner{runtime.getImmediateCaller()};
    OUTCOME_TRY(state, runtime.getCurrentActorStateCbor<State>());
    OUTCOME_TRY(pledge,
                computeInitialPledge(runtime, state, params.new_weight));
    OUTCOME_TRY(state.addToClaim(miner,
                                 -params.prev_weight.sector_size,
                                 -qaPowerForWeight(params.prev_weight)));
    OUTCOME_TRY(state.addToClaim(miner,
                                 params.new_weight.sector_size,
                                 qaPowerForWeight(params.new_weight)));
    OUTCOME_TRY(runtime.commitState(state));
    return std::move(pledge);
  }

  ACTOR_METHOD_IMPL(EnrollCronEvent) {
    OUTCOME_TRY(runtime.validateImmediateCallerIsMiner());
    OUTCOME_TRY(state, runtime.getCurrentActorStateCbor<State>());
    OUTCOME_TRY(state.appendCronEvent(
        params.event_epoch, {runtime.getImmediateCaller(), params.payload}));
    OUTCOME_TRY(runtime.commitState(state));
    return outcome::success();
  }

  ACTOR_METHOD_IMPL(OnEpochTickEnd) {
    OUTCOME_TRY(runtime.validateImmediateCallerIs(kCronAddress));
    OUTCOME_TRY(processDeferredCronEvents(runtime));
    OUTCOME_TRY(processBatchProofVerifies(runtime));

    OUTCOME_TRY(state, runtime.getCurrentActorStateCbor<State>());
    OUTCOME_TRY(runtime.sendM<reward::UpdateNetworkKPI>(
        kRewardAddress, state.total_raw_power, 0));
    return outcome::success();
  }

  ACTOR_METHOD_IMPL(UpdatePledgeTotal) {
    OUTCOME_TRY(runtime.validateImmediateCallerIsMiner());
    OUTCOME_TRY(state, runtime.getCurrentActorStateCbor<State>());
    OUTCOME_TRY(state.addPledgeTotal(params));
    OUTCOME_TRY(runtime.commitState(state));
    return outcome::success();
  }

  ACTOR_METHOD_IMPL(OnConsensusFault) {
    OUTCOME_TRY(runtime.validateImmediateCallerIs(kCronAddress));
    auto miner{runtime.getImmediateCaller()};
    OUTCOME_TRY(state, runtime.getCurrentActorStateCbor<State>());
    OUTCOME_TRY(claim, state.claims.get(miner));
    VM_ASSERT(claim.raw_power >= 0);
    VM_ASSERT(claim.qa_power >= 0);
    state.total_raw_power -= claim.raw_power;
    state.total_qa_power -= claim.qa_power;
    OUTCOME_TRY(state.addPledgeTotal(-params));
    OUTCOME_TRY(deleteMinerActor(state, miner));
    OUTCOME_TRY(runtime.commitState(state));
    return outcome::success();
  }

  ACTOR_METHOD_IMPL(SubmitPoRepForBulkVerify) {
    OUTCOME_TRY(runtime.validateImmediateCallerIsMiner());
    OUTCOME_TRY(state, runtime.getCurrentActorStateCbor<State>());
    if (!state.proof_validation_batch) {
      state.proof_validation_batch.reset({runtime});
    }
    OUTCOME_TRY(adt::Multimap::append(
        *state.proof_validation_batch, runtime.getImmediateCaller(), params));
    OUTCOME_TRY(runtime.commitState(state));
    return outcome::success();
  }

  const ActorExports exports{
      exportMethod<Construct>(),
      exportMethod<CreateMiner>(),
      exportMethod<DeleteMiner>(),
      exportMethod<OnSectorProveCommit>(),
      exportMethod<OnSectorTerminate>(),
      exportMethod<OnFaultBegin>(),
      exportMethod<OnFaultEnd>(),
      exportMethod<OnSectorModifyWeightDesc>(),
      exportMethod<EnrollCronEvent>(),
      exportMethod<OnEpochTickEnd>(),
      exportMethod<UpdatePledgeTotal>(),
      exportMethod<OnConsensusFault>(),
      exportMethod<SubmitPoRepForBulkVerify>(),
  };
}  // namespace fc::vm::actor::builtin::storage_power
