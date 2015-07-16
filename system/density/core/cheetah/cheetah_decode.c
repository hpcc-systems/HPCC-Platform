/*
 * Centaurean Density
 *
 * Copyright (c) 2013, Guillaume Voirin
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     1. Redistributions of source code must retain the above copyright notice, this
 *        list of conditions and the following disclaimer.
 *
 *     2. Redistributions in binary form must reproduce the above copyright notice,
 *        this list of conditions and the following disclaimer in the documentation
 *        and/or other materials provided with the distribution.
 *
 *     3. Neither the name of the copyright holder nor the names of its
 *        contributors may be used to endorse or promote products derived from
 *        this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * 24/06/15 0:32
 *
 * -----------------
 * Cheetah algorithm
 * -----------------
 *
 * Author(s)
 * Guillaume Voirin (https://github.com/gpnuma)
 * Piotr Tarsa (https://github.com/tarsa)
 *
 * Description
 * Very fast two level dictionary hash algorithm derived from Chameleon, with predictions lookup
 */

#include "cheetah_decode.h"

DENSITY_FORCE_INLINE void density_cheetah_decode_process_predicted(uint8_t **restrict out, uint_fast16_t *restrict last_hash, density_cheetah_dictionary *const restrict dictionary) {
    const uint32_t unit = dictionary->prediction_entries[*last_hash].next_chunk_prediction;
    DENSITY_MEMCPY(*out, &unit, sizeof(uint32_t));
    *last_hash = DENSITY_CHEETAH_HASH_ALGORITHM(unit);
}

DENSITY_FORCE_INLINE void density_cheetah_decode_process_compressed_a(uint8_t **restrict out, uint_fast16_t *restrict last_hash, density_cheetah_dictionary *const restrict dictionary, const uint16_t hash) {
    __builtin_prefetch(&dictionary->prediction_entries[hash]);
    const uint32_t unit = dictionary->entries[hash].chunk_a;
    DENSITY_MEMCPY(*out, &unit, sizeof(uint32_t));
    dictionary->prediction_entries[*last_hash].next_chunk_prediction = unit;
    *last_hash = hash;
}

DENSITY_FORCE_INLINE void density_cheetah_decode_process_compressed_b(uint8_t **restrict out, uint_fast16_t *restrict last_hash, density_cheetah_dictionary *const restrict dictionary, const uint16_t hash) {
    __builtin_prefetch(&dictionary->prediction_entries[hash]);
    density_cheetah_dictionary_entry *const entry = &dictionary->entries[hash];
    const uint32_t unit = entry->chunk_b;
    entry->chunk_b = entry->chunk_a;
    entry->chunk_a = unit;
    DENSITY_MEMCPY(*out, &unit, sizeof(uint32_t));
    dictionary->prediction_entries[*last_hash].next_chunk_prediction = unit;
    *last_hash = hash;
}

DENSITY_FORCE_INLINE void density_cheetah_decode_process_uncompressed(uint8_t **restrict out, uint_fast16_t *restrict last_hash, density_cheetah_dictionary *const restrict dictionary, const uint32_t unit) {
    const uint16_t hash = DENSITY_CHEETAH_HASH_ALGORITHM(unit);
    __builtin_prefetch(&dictionary->prediction_entries[hash]);
    density_cheetah_dictionary_entry *const entry = &dictionary->entries[hash];
    entry->chunk_b = entry->chunk_a;
    entry->chunk_a = unit;
    DENSITY_MEMCPY(*out, &unit, sizeof(uint32_t));
    dictionary->prediction_entries[*last_hash].next_chunk_prediction = unit;
    *last_hash = hash;
}

DENSITY_FORCE_INLINE void density_cheetah_decode_kernel_4(const uint8_t **restrict in, uint8_t **restrict out, uint_fast16_t *restrict last_hash, const uint8_t flag, density_cheetah_dictionary *const restrict dictionary) {
    uint16_t hash;
    uint32_t unit;

    switch (flag) {
        case DENSITY_CHEETAH_SIGNATURE_FLAG_PREDICTED:
            density_cheetah_decode_process_predicted(out, last_hash, dictionary);
            break;
        case DENSITY_CHEETAH_SIGNATURE_FLAG_MAP_A:
            DENSITY_MEMCPY(&hash, *in, sizeof(uint16_t));
            density_cheetah_decode_process_compressed_a(out, last_hash, dictionary, hash);
            *in += sizeof(uint16_t);
            break;
        case DENSITY_CHEETAH_SIGNATURE_FLAG_MAP_B:
            DENSITY_MEMCPY(&hash, *in, sizeof(uint16_t));
            density_cheetah_decode_process_compressed_b(out, last_hash, dictionary, hash);
            *in += sizeof(uint16_t);
            break;
        default:    // DENSITY_CHEETAH_SIGNATURE_FLAG_CHUNK
            DENSITY_MEMCPY(&unit, *in, sizeof(uint32_t));
            density_cheetah_decode_process_uncompressed(out, last_hash, dictionary, unit);
            *in += sizeof(uint32_t);
            break;
    }

    *out += sizeof(uint32_t);
}

DENSITY_FORCE_INLINE void density_cheetah_decode_kernel_16(const uint8_t **restrict in, uint8_t **restrict out, uint_fast16_t *restrict last_hash, const uint8_t flags, density_cheetah_dictionary *const restrict dictionary) {
    uint16_t hash;
    uint32_t unit;

    switch (flags) {
        DENSITY_CASE_GENERATOR_4_4_COMBINED(\
            density_cheetah_decode_process_predicted(out, last_hash, dictionary);, \
            DENSITY_CHEETAH_SIGNATURE_FLAG_PREDICTED, \
            DENSITY_MEMCPY(&hash, *in, sizeof(uint16_t)); \
            density_cheetah_decode_process_compressed_a(out, last_hash, dictionary, hash);\
            *in += sizeof(uint16_t);, \
            DENSITY_CHEETAH_SIGNATURE_FLAG_MAP_A, \
            DENSITY_MEMCPY(&hash, *in, sizeof(uint16_t)); \
            density_cheetah_decode_process_compressed_b(out, last_hash, dictionary, hash);\
            *in += sizeof(uint16_t);, \
            DENSITY_CHEETAH_SIGNATURE_FLAG_MAP_B, \
            DENSITY_MEMCPY(&unit, *in, sizeof(uint32_t)); \
            density_cheetah_decode_process_uncompressed(out, last_hash, dictionary, unit);\
            *in += sizeof(uint32_t);, \
            DENSITY_CHEETAH_SIGNATURE_FLAG_CHUNK, \
            *out += sizeof(uint32_t);, \
            2\
);
        default:
            break;
    }

    *out += sizeof(uint32_t);
}

DENSITY_FORCE_INLINE const uint8_t density_cheetah_decode_read_flag(const uint_fast64_t signature, const uint_fast8_t shift) {
    return (uint8_t const) ((signature >> shift) & 0x3);
}

DENSITY_FORCE_INLINE void density_cheetah_decode_4(const uint8_t **restrict in, uint8_t **restrict out, uint_fast16_t *restrict last_hash, const uint_fast64_t signature, const uint_fast8_t shift, density_cheetah_dictionary *const restrict dictionary) {
    density_cheetah_decode_kernel_4(in, out, last_hash, density_cheetah_decode_read_flag(signature, shift), dictionary);
}

DENSITY_FORCE_INLINE void density_cheetah_decode_16(const uint8_t **restrict in, uint8_t **restrict out, uint_fast16_t *restrict last_hash, const uint_fast64_t signature, const uint_fast8_t shift, density_cheetah_dictionary *const restrict dictionary) {
    density_cheetah_decode_kernel_16(in, out, last_hash, (uint8_t const) ((signature >> shift) & 0xff), dictionary);
}

DENSITY_FORCE_INLINE void density_cheetah_decode_128(const uint8_t **restrict in, uint8_t **restrict out, uint_fast16_t *restrict last_hash, const uint_fast64_t signature, density_cheetah_dictionary *const restrict dictionary) {
    uint_fast8_t count = 0;

#ifdef __clang__
    for (uint_fast8_t count_b = 0; count_b < 8; count_b ++) {
        density_cheetah_decode_16(in, out, last_hash, signature, count, dictionary);
        count += 8;
    }
#else
    for (uint_fast8_t count_b = 0; count_b < density_bitsizeof(density_cheetah_signature); count_b += 8)
        density_cheetah_decode_16(in, out, last_hash, signature, count_b, dictionary);
#endif
}

DENSITY_FORCE_INLINE void density_cheetah_decode_read_signature(const uint8_t **restrict in, density_cheetah_signature *restrict signature) {
    DENSITY_MEMCPY(signature, *in, sizeof(density_cheetah_signature));
    *in += sizeof(density_cheetah_signature);
}

DENSITY_WINDOWS_EXPORT DENSITY_FORCE_INLINE const bool density_cheetah_decode_unrestricted(const uint8_t **restrict in, const uint_fast64_t in_size, uint8_t **restrict out) {
    density_cheetah_signature signature;
    density_cheetah_dictionary dictionary;
    density_cheetah_dictionary_reset(&dictionary);
    uint_fast8_t shift;
    uint_fast64_t remaining;
    uint_fast16_t last_hash = 0;
    uint8_t flag;

    const uint8_t *start = *in;

    if (in_size < DENSITY_CHEETAH_MAXIMUM_COMPRESSED_UNIT_SIZE)
        goto read_signature;

    while (*in - start <= in_size - DENSITY_CHEETAH_MAXIMUM_COMPRESSED_UNIT_SIZE) {
        density_cheetah_decode_read_signature(in, &signature);
        density_cheetah_decode_128(in, out, &last_hash, signature, &dictionary);
    }

    read_signature:
    if (in_size - (*in - start) < sizeof(density_cheetah_signature))
        return false;
    shift = 0;
    density_cheetah_decode_read_signature(in, &signature);
    read_and_decode_4:
    switch (in_size - (*in - start)) {
        case 0:
        case 1:
            switch (density_cheetah_decode_read_flag(signature, shift)) {
                case DENSITY_CHEETAH_SIGNATURE_FLAG_CHUNK:
                    goto process_remaining_bytes;   // End marker
                case DENSITY_CHEETAH_SIGNATURE_FLAG_PREDICTED:
                    density_cheetah_decode_kernel_4(in, out, &last_hash, DENSITY_CHEETAH_SIGNATURE_FLAG_PREDICTED, &dictionary);
                    shift += 2;
                    break;
                default:
                    return false;
            }
            break;
        case 2:
        case 3:
            flag = density_cheetah_decode_read_flag(signature, shift);
            switch (flag) {
                case DENSITY_CHEETAH_SIGNATURE_FLAG_CHUNK:
                    goto process_remaining_bytes;   // End marker
                default:
                    density_cheetah_decode_kernel_4(in, out, &last_hash, flag, &dictionary);
                    shift += 2;
                    break;
            }
            break;
        default:
            density_cheetah_decode_4(in, out, &last_hash, signature, shift, &dictionary);
            shift += 2;
            break;
    }

    if (density_unlikely(shift == density_bitsizeof(density_cheetah_signature)))
        goto read_signature;
    else
        goto read_and_decode_4;

    process_remaining_bytes:
    remaining = in_size - (*in - start);
    DENSITY_MEMCPY(*out, *in, remaining);
    *in += remaining;
    *out += remaining;
    return true;
}