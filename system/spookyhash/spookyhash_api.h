/*
 * Centaurean SpookyHash
 *
 * Copyright (c) 2015, Guillaume Voirin
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
 * 26/06/15 0:40
 *
 * ----------
 * SpookyHash
 * ----------
 *
 * Author(s)
 * Bob Jenkins (http://burtleburtle.net/bob/hash/spooky.html)
 *
 * Description
 * Very fast non cryptographic hash
 */

#ifndef SPOOKYHASH_API_H
#define SPOOKYHASH_API_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stddef.h>

#if defined(_WIN64) || defined(_WIN32)
#define SPOOKYHASH_WINDOWS_EXPORT __declspec(dllexport)
#else
#define SPOOKYHASH_WINDOWS_EXPORT
#endif

/***********************************************************************************************************************
 *                                                                                                                     *
 * SpookyHash data structures                                                                                          *
 *                                                                                                                     *
 ***********************************************************************************************************************/

#define SPOOKYHASH_VARIABLES (12)

typedef struct {
    uint64_t m_data[2 * SPOOKYHASH_VARIABLES];
    uint64_t m_state[SPOOKYHASH_VARIABLES];
    size_t m_length;
    uint8_t m_remainder;
} spookyhash_context;


/***********************************************************************************************************************
 *                                                                                                                     *
 * SpookyHash version information                                                                                      *
 *                                                                                                                     *
 ***********************************************************************************************************************/

/*
 * Returns the major version
 */
SPOOKYHASH_WINDOWS_EXPORT uint8_t spookyhash_version_major(void);

/*
 * Returns the minor version
 */
SPOOKYHASH_WINDOWS_EXPORT uint8_t spookyhash_version_minor(void);

/*
 * Returns the revision
 */
SPOOKYHASH_WINDOWS_EXPORT uint8_t spookyhash_version_revision(void);

#ifdef __cplusplus
}
#endif


/***********************************************************************************************************************
 *                                                                                                                     *
 * SpookyHash context setup                                                                                            *
 *                                                                                                                     *
 ***********************************************************************************************************************/

/*
 * Initialize a context
 *
 * @param context a SpookyHash context structure
 * @param seed_1 the first 8 bytes of the seed
 * @param seed_2 the last 8 bytes of the seed
 */
SPOOKYHASH_WINDOWS_EXPORT void spookyhash_context_init(spookyhash_context *context, uint64_t seed_1, uint64_t seed_2);


/***********************************************************************************************************************
 *                                                                                                                     *
 * SpookyHash main API functions                                                                                       *
 *                                                                                                                     *
 ***********************************************************************************************************************/

/*
 * Get a direct 128 bit hash
 *
 * @param input a given buffer
 * @param input_size the size of our buffer
 * @param hash_1 used on call as a the first 8 bytes of the seed, returns the first 8 bytes of the resulting hash
 * @param hash_2 used on call as a the last 8 bytes of the seed, returns the last 8 bytes of the resulting hash
 */
SPOOKYHASH_WINDOWS_EXPORT void spookyhash_128(const void *input, size_t input_size, uint64_t *hash_1, uint64_t *hash_2);

/*
 * Get a direct 64 bit hash
 *
 * @param input a given buffer
 * @param input_size the size of our buffer
 * @param seed the 8 byte seed
 *
 * @returns an 8 byte hash
 */
SPOOKYHASH_WINDOWS_EXPORT uint64_t spookyhash_64(const void *input, size_t input_size, uint64_t seed);

/*
 * Get a direct 32 bit hash
 *
 * @param input a given buffer
 * @param input_size the size of our buffer
 * @param seed the 4 byte seed
 *
 * @returns a 4 byte hash
 */
SPOOKYHASH_WINDOWS_EXPORT uint32_t spookyhash_32(const void *input, size_t input_size, uint32_t seed);

/*
 * Update a hash calculation, using the given context
 *
 * @param context the calculation context (has to be initialized using @spookyhash_context_init)
 * @param input a given buffer
 * @param input_size the size of our buffer
 */
SPOOKYHASH_WINDOWS_EXPORT void spookyhash_update(spookyhash_context *context, const void *input, size_t input_size);

/*
 * Finish a hash calculation, using the given context
 *
 * @param context the calculation context (has to be initialized using @spookyhash_context_init)
 * @param hash_1 returns the first 8 bytes of the resulting hash
 * @param hash_2 returns the last 8 bytes of the resulting hash
 */
SPOOKYHASH_WINDOWS_EXPORT void spookyhash_final(spookyhash_context *context, uint64_t *hash_1, uint64_t *hash_2);

#endif
