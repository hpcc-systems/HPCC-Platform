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

// Symmetric-key encryption

#ifndef SKE_HPP
#define SKE_HPP

#ifndef CRYPTOHELPER_API

#ifndef CRYPTOHELPER_EXPORTS
    #define CRYPTOHELPER_API DECL_IMPORT
#else
    #define CRYPTOHELPER_API DECL_EXPORT
#endif //CRYPTOHELPER_EXPORTS

#endif

namespace cryptohelper
{

#if defined(_USE_OPENSSL) && !defined(_WIN32)

const unsigned aesMaxKeySize = 256/8; // 256 bits
const unsigned aesBlockSize = 128/8; // 128 bits

// for AES, keyLen must be 16, 24, or 32 Bytes

CRYPTOHELPER_API size32_t aesEncrypt(MemoryBuffer &out, size32_t inSz, const void *inBytes, size32_t keyLen, const char *key, const char iv[aesBlockSize] = nullptr);
CRYPTOHELPER_API size32_t aesDecrypt(MemoryBuffer &out, size32_t inSz, const void *inBytes, size32_t keyLen, const char *key, const char iv[aesBlockSize] = nullptr);

class CLoadedKey;
// aesEncryptWithRSAEncryptedKey serializes encrypted data along with an RSA encrypted key in the format { RSA-encrypted-AES-key, aes-IV, AES-encrypted-data }
CRYPTOHELPER_API size32_t aesEncryptWithRSAEncryptedKey(MemoryBuffer &out, size32_t inSz, const void *inBytes, const CLoadedKey &publicKey);
// aesDecryptWithRSAEncryptedKey deserializes data created by aesEncryptWithRSAEncryptedKey
CRYPTOHELPER_API size32_t aesDecryptWithRSAEncryptedKey(MemoryBuffer &out, size32_t inSz, const void *inBytes, const CLoadedKey &privateKey);


#endif // end of #if defined(_USE_OPENSSL) && !defined(_WIN32)

} // end of namespace cryptohelper


#endif // SKE_HPP

