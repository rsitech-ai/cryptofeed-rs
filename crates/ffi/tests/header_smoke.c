#include "marketfeed.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

_Static_assert(sizeof(mf_fixed) == 24, "mf_fixed ABI size changed");
_Static_assert(offsetof(mf_fixed, coefficient_lo) == 0, "coefficient_lo offset changed");
_Static_assert(offsetof(mf_fixed, coefficient_hi) == 8, "coefficient_hi offset changed");
_Static_assert(offsetof(mf_fixed, scale) == 16, "scale offset changed");
_Static_assert(MF_OK == 0, "MF_OK changed");
_Static_assert(MF_ERR_INEXACT == 6, "MF_ERR_INEXACT changed");

static const char *(*version_fn)(void) = marketfeed_version;
static int (*parse_fn)(const char *, size_t, mf_fixed *) = marketfeed_fixed_parse;
static int (*parse_cstr_fn)(const char *, mf_fixed *) = marketfeed_fixed_parse_cstr;

int main(void) {
    mf_fixed parsed = {0};
    const char *version = version_fn();
    if (version == NULL || strlen(version) == 0) {
        return 1;
    }
    if (parse_fn("123.45", 6, &parsed) != MF_OK ||
        parsed.coefficient_lo != 12345 ||
        parsed.coefficient_hi != 0 ||
        parsed.scale != 2) {
        return 2;
    }
    if (parse_cstr_fn("-0.001", &parsed) != MF_OK ||
        parsed.coefficient_lo != UINT64_MAX ||
        parsed.coefficient_hi != -1 ||
        parsed.scale != 3) {
        return 3;
    }
    if (parse_fn(NULL, 0, &parsed) != MF_ERR_EMPTY ||
        parse_cstr_fn(NULL, &parsed) != MF_ERR_NULL) {
        return 4;
    }
    return 0;
}
