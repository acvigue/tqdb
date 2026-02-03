/**
 * @file tqdb_crc32.c
 * @brief CRC32 implementation using polynomial calculation (no lookup table)
 *
 * Uses bit-by-bit polynomial calculation instead of 1KB lookup table
 * to minimize code size for embedded systems.
 */

#include "tqdb_internal.h"

/* Standard CRC-32 polynomial (IEEE 802.3) */
#define CRC32_POLYNOMIAL 0xEDB88320u

uint32_t tqdb_crc32_update(uint32_t crc, const uint8_t* data, size_t len) {
    while (len--) {
        crc ^= *data++;
        for (int i = 0; i < 8; i++) {
            crc = (crc >> 1) ^ ((crc & 1) ? CRC32_POLYNOMIAL : 0);
        }
    }
    return crc;
}

uint32_t tqdb_crc32_finalize(uint32_t crc) {
    return ~crc;
}
