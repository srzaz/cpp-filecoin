#
# Copyright Soramitsu Co., Ltd. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#

addtest(chain_events_test
    chain_events_test.cpp
    )
target_link_libraries(chain_events_test
    chain_events
    )
