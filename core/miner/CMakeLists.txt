#
# Copyright Soramitsu Co., Ltd. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#

add_subdirectory(storage_fsm)

add_library(miner
    miner.cpp
    )
target_link_libraries(miner
    api
    storage_fsm
    )

add_library(mining
    mining.cpp
    windowpost.cpp
    )
target_link_libraries(mining
    cbor
    message
    rle_plus_codec
    tipset
    )
