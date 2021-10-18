#include "gcd.h"

int test::calc(int a, int b) {
    return b == 0 ? a : calc(b, a % b);
}