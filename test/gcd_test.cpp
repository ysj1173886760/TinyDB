#include <gtest/gtest.h>
#include "gcd.h"

TEST(GCDTEST, BasicTest) {
    test t;
    EXPECT_EQ(t.calc(84, 35), 7);
}