/* marketfeed C ABI stub — version + Fixed parse only (no full API).
 * Keep in sync with crates/ffi/src/lib.rs. No cbindgen in-tree.
 */
#ifndef MARKETFEED_H
#define MARKETFEED_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct mf_fixed {
    uint64_t coefficient_lo;
    int64_t coefficient_hi;
    uint8_t scale;
} mf_fixed;

enum {
    MF_OK = 0,
    MF_ERR_NULL = 1,
    MF_ERR_EMPTY = 2,
    MF_ERR_SYNTAX = 3,
    MF_ERR_OVERFLOW = 4,
    MF_ERR_SCALE = 5,
    MF_ERR_INEXACT = 6
};

const char *marketfeed_version(void);
int marketfeed_fixed_parse(const char *ptr, size_t len, mf_fixed *out);
int marketfeed_fixed_parse_cstr(const char *s, mf_fixed *out);

#ifdef __cplusplus
}
#endif

#endif /* MARKETFEED_H */
