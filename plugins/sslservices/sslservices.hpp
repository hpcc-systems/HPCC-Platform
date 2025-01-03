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

#ifndef _SSLSERVICES_INCL
#define _SSLSERVICES_INCL

#ifdef _WIN32
#define SSLSERVICES_CALL _cdecl
#else
#define SSLSERVICES_CALL
#endif

#ifdef SSLSERVICES_EXPORTS
#define SSLSERVICES_API DECL_EXPORT
#else
#define SSLSERVICES_API DECL_IMPORT
#endif

#include "platform.h"
#include "jthread.hpp"
#include "hqlplugins.hpp"
#include "eclrtl_imp.hpp"
#include "eclhelper.hpp"

extern "C"
{
    SSLSERVICES_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb);
}

extern "C++"
{
// Digest functions
SSLSERVICES_API void SSLSERVICES_CALL digestAvailableAlgorithms(ICodeContext *ctx, size32_t & __lenResult, void * & __result);
SSLSERVICES_API void SSLSERVICES_CALL digestHash(ICodeContext *ctx, size32_t & __lenResult, void * & __result, size32_t len_indata, const void * _indata, const char * _algorithm_name);

// Cipher functions
SSLSERVICES_API void SSLSERVICES_CALL cipherAvailableAlgorithms(ICodeContext *ctx, size32_t & __lenResult, void * & __result);
SSLSERVICES_API uint16_t SSLSERVICES_CALL cipherIVSize(ICodeContext *ctx, const char * _algorithm_name);
SSLSERVICES_API void SSLSERVICES_CALL cipherEncrypt(ICodeContext *ctx, size32_t & __lenResult, void * & __result, size32_t len_plaintext, const void * _plaintext, const char * _algorithm_name, size32_t len_passphrase, const void * _passphrase, size32_t len_iv, const void * _iv, size32_t len_salt, const void * _salt);
SSLSERVICES_API void SSLSERVICES_CALL cipherDecrypt(ICodeContext *ctx, size32_t & __lenResult, void * & __result, size32_t len_ciphertext, const void * _ciphertext, const char * _algorithm_name, size32_t len_passphrase, const void * _passphrase, size32_t len_iv, const void * _iv, size32_t len_salt, const void * _salt);

// Public Key functions
SSLSERVICES_API void SSLSERVICES_CALL pkRSASeal(ICodeContext *ctx, size32_t & __lenResult, void * & __result, size32_t len_plaintext, const void * _plaintext, bool isAll_pem_public_keys, size32_t len_pem_public_keys, const void * _pem_public_keys, const char * _algorithm_name);
SSLSERVICES_API void SSLSERVICES_CALL pkRSAUnseal(ICodeContext *ctx, size32_t & __lenResult, void * & __result, size32_t len_ciphertext, const void * _ciphertext, size32_t len_passphrase, const void * _passphrase, size32_t len_pem_private_key, const char * _pem_private_key, const char * _algorithm_name);
SSLSERVICES_API void SSLSERVICES_CALL pkEncrypt(ICodeContext *ctx, size32_t & __lenResult, void * & __result, size32_t len_plaintext, const void * _plaintext, size32_t len_pem_public_key, const char * _pem_public_key);
SSLSERVICES_API void SSLSERVICES_CALL pkDecrypt(ICodeContext *ctx, size32_t & __lenResult, void * & __result, size32_t len_ciphertext, const void * _ciphertext, size32_t len_passphrase, const void * _passphrase, size32_t len_pem_private_key, const char * _pem_private_key);
SSLSERVICES_API void SSLSERVICES_CALL pkSign(ICodeContext *ctx, size32_t & __lenResult, void * & __result, size32_t len_plaintext, const void * _plaintext, size32_t len_passphrase, const void * _passphrase, size32_t len_pem_private_key, const char * _pem_private_key, const char * _algorithm_name);
SSLSERVICES_API bool SSLSERVICES_CALL pkVerifySignature(ICodeContext *ctx, size32_t len_signature, const void * _signature, size32_t len_signedData, const void * _signedData, size32_t len_pem_public_key, const char * _pem_public_key, const char * _algorithm_name);
}

#endif // ECL_SSLSERVICES_INCL
