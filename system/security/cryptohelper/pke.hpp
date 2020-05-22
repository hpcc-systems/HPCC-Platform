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

#if defined(_USE_OPENSSL)

#include "cryptocommon.hpp"

namespace cryptohelper
{

class jlib_decl CLoadedKey : public CSimpleInterfaceOf<IInterface>
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
    CLoadedKey(const CLoadedKey&) = delete;
    inline EVP_PKEY *get() const           { return key; }
    inline EVP_PKEY * operator -> () const { return key; }
    inline operator EVP_PKEY *() const     { return key; }
    const char *queryKeyName() const  { return keyName; }
};

jlib_decl CLoadedKey *loadPublicKeyFromFile(const char *keyFile);
jlib_decl CLoadedKey *loadPublicKeyFromMemory(const char *key);
jlib_decl CLoadedKey *loadPrivateKeyFromFile(const char *keyFile, const char *passPhrase);
jlib_decl CLoadedKey *loadPrivateKeyFromMemory(const char *key, const char *passPhrase);

//binary passphase, possibly containing embedded NULL characters
jlib_decl CLoadedKey *loadPrivateKeyFromFile(const char *keyFile, size32_t passPhraseLen, const void *passPhrase);
jlib_decl CLoadedKey *loadPrivateKeyFromMemory(const char *key, size32_t passPhraseLen, const void *passPhrase);

jlib_decl size32_t publicKeyEncrypt(MemoryBuffer &out, size32_t inLen, const void *inBytes, const CLoadedKey &key);
jlib_decl size32_t privateKeyDecrypt(MemoryBuffer &out, size32_t inLen, const void *inBytes, const CLoadedKey &key);
jlib_decl size32_t publicKeyEncrypt(void *dst, size32_t dstMaxSz, size32_t inLen, const void *inBytes, const CLoadedKey &key);
jlib_decl size32_t privateKeyDecrypt(void *dst, size32_t dstMaxSz, size32_t inLen, const void *inBytes, const CLoadedKey &key);

} // end of namespace cryptohelper

#else
class CLoadedKey;
#endif // end of #if defined(_USE_OPENSSL)

#endif // PKE_HPP

