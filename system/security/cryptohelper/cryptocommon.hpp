/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

#ifndef CRYPTOCOMMON_HPP
#define CRYPTOCOMMON_HPP

#if defined(_USE_OPENSSL) && !defined(_WIN32)

#include <openssl/pem.h>
#include <openssl/evp.h>
#include <openssl/err.h>

#include "jiface.hpp"
#include "jbuff.hpp"

namespace cryptohelper
{

jlib_decl IException *makeEVPException(int code, const char *msg);
jlib_decl IException *makeEVPExceptionV(int code, const char *format, ...) __attribute__((format(printf, 2, 3)));
jlib_decl void throwEVPException(int code, const char *format);
jlib_decl void throwEVPExceptionV(int code, const char *format, ...) __attribute__((format(printf, 2, 3)));

inline void voidBIOfree(BIO *bio) { BIO_free(bio); }
inline void voidOpenSSLFree(void *m) { OPENSSL_free(m); }

typedef OwnedPtrCustomFree<BIO, voidBIOfree> OwnedEVPBio;
typedef OwnedPtrCustomFree<EVP_PKEY, EVP_PKEY_free> OwnedEVPPkey;
typedef OwnedPtrCustomFree<EVP_PKEY_CTX, EVP_PKEY_CTX_free> OwnedEVPPkeyCtx;
typedef OwnedPtrCustomFree<void, voidOpenSSLFree> OwnedEVPMemory;
typedef OwnedPtrCustomFree<EVP_CIPHER_CTX, EVP_CIPHER_CTX_free> OwnedEVPCipherCtx;
typedef OwnedPtrCustomFree<RSA, RSA_free> OwnedEVPRSA;
#if OPENSSL_VERSION_NUMBER < 0x10100000L
typedef OwnedPtrCustomFree<EVP_MD_CTX, EVP_MD_CTX_destroy> OwnedEVPMdCtx;
#else
typedef OwnedPtrCustomFree<EVP_MD_CTX, EVP_MD_CTX_free> OwnedEVPMdCtx;
#endif

} // end of namespace cryptohelper

#endif // end of #if defined(_USE_OPENSSL) && !defined(_WIN32)

#endif // CRYPTOCOMMON_HPP

