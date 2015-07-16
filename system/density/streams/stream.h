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
 * 30/06/13 10:59
 */

#include "../density_api.h"

#pragma pack(push)
#pragma pack(4)
typedef struct {
    //DENSITY_ENCODE_PROCESS process;
    DENSITY_COMPRESSION_MODE compressionMode;
    DENSITY_BLOCK_TYPE blockType;

    uint_fast64_t totalRead;
    uint_fast64_t totalWritten;
} density_stream_encode_state;

typedef struct {
    //DENSITY_STREAM_PROCESS process;
    density_byte temporary_buffer[1 << 16];
    uint_fast16_t available_bytes;

    density_stream_encode_state internal_encode_state;
    //density_decode_state internal_decode_state;
} density_stream_state;
#pragma pack(pop)

DENSITY_WINDOWS_EXPORT DENSITY_STREAM_STATE density_stream_compress_init(density_stream *const, const DENSITY_COMPRESSION_MODE, const DENSITY_BLOCK_TYPE);

DENSITY_WINDOWS_EXPORT DENSITY_STREAM_STATE density_stream_compress_continue(density_stream *const, const uint8_t *const, const uint_fast64_t, uint8_t *const, const uint_fast64_t);