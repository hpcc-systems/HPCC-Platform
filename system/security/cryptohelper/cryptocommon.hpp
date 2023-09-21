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

#if defined(_USE_OPENSSL)

#include <opensslcommon.hpp>
#include <openssl/ssl.h>
#include <openssl/pem.h>
#include <openssl/evp.h>
#include <openssl/err.h>
#include <openssl/rsa.h>
#include <openssl/x509.h>

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
inline void voidSSLCTXfree(SSL_CTX *ctx)
{
    if (ctx)
        SSL_CTX_free(ctx);
}
inline void voidX509StoreFree(X509_STORE *store)
{
    if (store)
        X509_STORE_free(store);
}
inline void voidX509StkPopFree(STACK_OF(X509_INFO) *infoStk)
{
    if (infoStk)
        sk_X509_INFO_pop_free(infoStk, X509_INFO_free);
}

typedef OwnedPtrCustomFree<X509_STORE, voidX509StoreFree> OwnedX509Store;
typedef OwnedPtrCustomFree<STACK_OF(X509_INFO), voidX509StkPopFree> OwnedX509StkPtr;
typedef OwnedPtrCustomFree<SSL_CTX, voidSSLCTXfree> OwnedSSLCTX;
typedef OwnedPtrCustomFree<BIO, voidBIOfree> OwnedEVPBio;
typedef OwnedPtrCustomFree<EVP_PKEY, EVP_PKEY_free> OwnedEVPPkey;
typedef OwnedPtrCustomFree<EVP_PKEY_CTX, EVP_PKEY_CTX_free> OwnedEVPPkeyCtx;
typedef OwnedPtrCustomFree<void, voidOpenSSLFree> OwnedEVPMemory;
typedef OwnedPtrCustomFree<EVP_CIPHER_CTX, EVP_CIPHER_CTX_free> OwnedEVPCipherCtx;
typedef OwnedPtrCustomFree<RSA, RSA_free> OwnedEVPRSA;
typedef OwnedPtrCustomFree<X509, X509_free> OwnedX509;
#if OPENSSL_VERSION_NUMBER < 0x10100000L
typedef OwnedPtrCustomFree<EVP_MD_CTX, EVP_MD_CTX_destroy> OwnedEVPMdCtx;
#else
typedef OwnedPtrCustomFree<EVP_MD_CTX, EVP_MD_CTX_free> OwnedEVPMdCtx;
#endif

} // end of namespace cryptohelper

#endif // end of #if defined(_USE_OPENSSL)

#endif // CRYPTOCOMMON_HPP

