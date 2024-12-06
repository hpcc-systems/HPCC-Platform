/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#ifndef _OPENSSL_INCL
#define _OPENSSL_INCL

#ifdef _WIN32
#define OPENSSL_CALL _cdecl
#else
#define OPENSSL_CALL
#endif

#ifdef OPENSSL_EXPORTS
#define OPENSSL_API DECL_EXPORT
#else
#define OPENSSL_API DECL_IMPORT
#endif

#include "platform.h"
#include "jthread.hpp"
#include "hqlplugins.hpp"
#include "eclrtl_imp.hpp"
#include "eclhelper.hpp"

extern "C++"
{
OPENSSL_API bool OPENSSL_CALL getECLPluginDefinition(ECLPluginDefinitionBlock *pb);

// Digest functions
OPENSSL_API void OPENSSL_CALL digestAvailableAlgorithms(ICodeContext *ctx, size32_t & __lenResult, void * & __result);
OPENSSL_API void OPENSSL_CALL digestHash(ICodeContext *ctx, size32_t & __lenResult, void * & __result, size32_t len_indata, const void * _indata, const char * _hash_name);

// Cipher functions
OPENSSL_API void OPENSSL_CALL cipherAvailableAlgorithms(ICodeContext *ctx, size32_t & __lenResult, void * & __result);
OPENSSL_API uint16_t OPENSSL_CALL cipherIVSize(ICodeContext *ctx, const char * algorithm);
OPENSSL_API void OPENSSL_CALL cipherEncrypt(ICodeContext *ctx, size32_t & __lenResult, void * & __result, size32_t len_plaintext, const void * _plaintext, const char * _algorithm, size32_t len_passphrase, const void * _passphrase, size32_t len_iv, const void * _iv, size32_t len_salt, const void * _salt);
OPENSSL_API void OPENSSL_CALL cipherDecrypt(ICodeContext *ctx, size32_t & __lenResult, void * & __result, size32_t len_ciphertext, const void * _ciphertext, const char * _algorithm, size32_t len_passphrase, const void * _passphrase, size32_t len_iv, const void * _iv, size32_t len_salt, const void * _salt);

// RSA functions
OPENSSL_API void OPENSSL_CALL rsaSeal(ICodeContext *ctx, size32_t & __lenResult, void * & __result, size32_t len_plaintext, const void * _plaintext, bool isAll_pem_public_keys, size32_t len_pem_public_keys, const void * _pem_public_keys, const char * _symmetric_algorithm);
OPENSSL_API void OPENSSL_CALL rsaUnseal(ICodeContext *ctx, size32_t & __lenResult, void * & __result, size32_t len_ciphertext, const void * _ciphertext, size32_t len_passphrase, const void * _passphrase, size32_t len_pem_private_key, const char * _pem_private_key, const char * _symmetric_algorithm);
OPENSSL_API void OPENSSL_CALL rsaEncrypt(ICodeContext *ctx, size32_t & __lenResult, void * & __result, size32_t len_plaintext, const void * _plaintext, size32_t len_pem_public_key, const char * _pem_public_key);
OPENSSL_API void OPENSSL_CALL rsaDecrypt(ICodeContext *ctx, size32_t & __lenResult, void * & __result, size32_t len_ciphertext, const void * _ciphertext, size32_t len_passphrase, const void * _passphrase, size32_t len_pem_private_key, const char * _pem_private_key);
OPENSSL_API void OPENSSL_CALL rsaSign(ICodeContext *ctx, size32_t & __lenResult, void * & __result, size32_t len_plaintext, const void * _plaintext, size32_t len_passphrase, const void * _passphrase, size32_t len_pem_private_key, const char * _pem_private_key, const char * _algorithm);
OPENSSL_API bool OPENSSL_CALL rsaVerifySignature(ICodeContext *ctx, size32_t len_signature, const void * _signature, size32_t len_signedData, const void * _signedData, size32_t len_pem_public_key, const char * _pem_public_key, const char * _algorithm);
}

#endif // ECL_OPENSSL_INCL
