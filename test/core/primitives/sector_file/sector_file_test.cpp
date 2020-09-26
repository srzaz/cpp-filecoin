/**
 * Copyright Soramitsu Co., Ltd. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "primitives/sector_file/sector_file.hpp"
#include <gtest/gtest.h>

#include "testutil/outcome.hpp"

using namespace fc::primitives::sector_file;

/**
 * @given Seal Proof type and Sector File type
 * @when try to get amount of used memory for sealing
 * @then get amount of used memory for this configuration
 */
TEST(SealSpaceUse, Success) {
  RegisteredProof seal_proof_type = RegisteredProof::StackedDRG2KiBSeal;
  SectorFileType file_type = SectorFileType::FTCache;
  EXPECT_OUTCOME_TRUE(sector_size,
                      fc::primitives::sector::getSectorSize(seal_proof_type));
  uint64_t result = kOverheadSeal.at(file_type) * sector_size / kOverheadDenominator;
  EXPECT_OUTCOME_TRUE(seal_size, sealSpaceUse(file_type, seal_proof_type));
  ASSERT_EQ(result, seal_size);
}
