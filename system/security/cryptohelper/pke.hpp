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

// Public-key encryption

#ifndef PKE_HPP
#define PKE_HPP

#ifndef CRYPTOHELPER_API

#ifndef CRYPTOHELPER_EXPORTS
    #define CRYPTOHELPER_API DECL_IMPORT
#else
    #define CRYPTOHELPER_API DECL_EXPORT
#endif //CRYPTOHELPER_EXPORTS

#endif

#include "cryptocommon.hpp"

namespace cryptohelper
{

#if defined(_USE_OPENSSL) && !defined(_WIN32)

#include <openssl/evp.h>
#include <openssl/err.h>

class CLoadedKey : public CSimpleInterfaceOf<IInterface>
{
protected:
    MemoryBuffer keyMb;
    OwnedEVPRSA rsa;
    OwnedEVPBio keyBio;
    OwnedEVPPkey key;
    StringAttr keyName;

    void loadKeyBio(size32_t keyLen, const char *keyMem);
    void loadKeyFromMem(const char *key);
    bool loadKeyFromFile(const char *keyFile);
    void finalize(RSA *rsaKey, const char *keyName);
public:
    CLoadedKey() { }
    inline EVP_PKEY *get() const           { return key; }
    inline EVP_PKEY * operator -> () const { return key; }
    inline operator EVP_PKEY *() const     { return key; }
    const char *queryKeyName() const  { return keyName; }
};

CRYPTOHELPER_API CLoadedKey *loadPublicKeyFromFile(const char *keyFile, const char *passPhrase);
CRYPTOHELPER_API CLoadedKey *loadPublicKeyFromMemory(const char *key, const char *passPhrase);
CRYPTOHELPER_API CLoadedKey *loadPrivateKeyFromFile(const char *keyFile, const char *passPhrase);
CRYPTOHELPER_API CLoadedKey *loadPrivateKeyFromMemory(const char *key, const char *passPhrase);

CRYPTOHELPER_API size32_t publicKeyEncrypt(MemoryBuffer &out, size32_t inLen, const void *inBytes, const CLoadedKey &key);
CRYPTOHELPER_API size32_t privateKeyDecrypt(MemoryBuffer &out, size32_t inLen, const void *inBytes, const CLoadedKey &key);
CRYPTOHELPER_API size32_t publicKeyEncrypt(void *dst, size32_t dstMaxSz, size32_t inLen, const void *inBytes, const CLoadedKey &key);
CRYPTOHELPER_API size32_t privateKeyDecrypt(void *dst, size32_t dstMaxSz, size32_t inLen, const void *inBytes, const CLoadedKey &key);


#endif // end of #if defined(_USE_OPENSSL) && !defined(_WIN32)

} // end of namespace cryptohelper


#endif // PKE_HPP

