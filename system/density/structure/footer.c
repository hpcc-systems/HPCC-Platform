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
 * 18/10/13 22:30
 */

#include "footer.h"

DENSITY_WINDOWS_EXPORT DENSITY_FORCE_INLINE void density_footer_read_unrestricted(const uint8_t **restrict in, density_footer *restrict footer) {
    DENSITY_MEMCPY(&footer->hashsum1, *in, sizeof(uint64_t));
    *in += sizeof(uint64_t);
    DENSITY_MEMCPY(&footer->hashsum2, *in, sizeof(uint64_t));
    *in += sizeof(uint64_t);
}

DENSITY_WINDOWS_EXPORT DENSITY_FORCE_INLINE void density_footer_write_unrestricted(uint8_t **restrict out, const uint_fast64_t hashsum1, const uint_fast64_t hashsum2) {
    DENSITY_MEMCPY(*out, &hashsum1, sizeof(uint64_t));
    *out += sizeof(uint64_t);
    DENSITY_MEMCPY(*out, &hashsum2, sizeof(uint64_t));
    *out += sizeof(uint64_t);
}