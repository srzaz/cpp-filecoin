/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "vm/actor/builtin/init/init_actor.hpp"

#include "adt/address_key.hpp"
#include "storage/hamt/hamt.hpp"
#include "vm/runtime/gas_cost.hpp"

namespace fc::vm::actor::builtin::init {

  outcome::result<Address> InitActorState::addActor(const Address &address) {
    auto id = next_id;
    OUTCOME_TRY(address_map.set(address, id));
    ++next_id;
    return Address::makeFromId(id);
  }

  ACTOR_METHOD_IMPL(Exec) {
    OUTCOME_TRY(caller, runtime.getActorCodeID(runtime.getImmediateCaller()));
    if ((params.code != kStorageMinerCodeCid || caller != kStoragePowerCodeCid)
        && params.code != kPaymentChannelCodeCid
        && params.code != kMultisigCodeCid) {
      return VMExitCode::kErrForbidden;
    }
    OUTCOME_TRY(runtime.chargeGas(runtime::kInitActorExecCost));
    OUTCOME_TRY(actor_address, runtime.newActorAddress());
    OUTCOME_TRY(init_actor, runtime.getCurrentActorStateCbor<InitActorState>());
    OUTCOME_TRY(id_address, init_actor.addActor(actor_address));
    OUTCOME_TRY(runtime.commitState(init_actor));
    OUTCOME_TRY(runtime.createActor(id_address,
                                    Actor{params.code, kEmptyObjectCid, 0, 0}));
    OUTCOME_TRY(runtime.send(id_address,
                             kConstructorMethodNumber,
                             params.params,
                             runtime.getValueReceived()));
    return Result{id_address, actor_address};
  }

  const ActorExports exports{
      exportMethod<Exec>(),
  };
}  // namespace fc::vm::actor::builtin::init
