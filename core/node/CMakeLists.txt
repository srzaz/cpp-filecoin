#
# Copyright Soramitsu Co., Ltd. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#

add_library(node
    blocksync.cpp
    hello.cpp
    peermgr.cpp
    sync.cpp
    )
target_link_libraries(node
    cbor_stream
    )

add_executable(node_main
    main.cpp
    )
target_link_libraries(node_main
    car
    drand_beacon
    in_memory_storage
    ipfs_datastore_in_memory
    mpool
    node
    p2p::asio_scheduler
    p2p::p2p_identify
    p2p::p2p_gossip
    rpc
    )
