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

/*
 * Module dealing with openssl symmetric key encryption/decription.
 * For now just AES.
 */

#if defined(_USE_OPENSSL) && !defined(_WIN32)

#include "jliball.hpp"
#include <openssl/pem.h>
#include <openssl/err.h>
#include <openssl/evp.h>

#include "cryptocommon.hpp"
#include "pke.hpp"
#include "ske.hpp"

static const EVP_CIPHER *getAesCipher(size32_t keyLen)
{
    switch (keyLen)
    {
    case 128/8:
        return EVP_aes_128_cbc();
    case 192/8:
        return EVP_aes_192_cbc();
    case 256/8:
        return EVP_aes_256_cbc();
    default:
        throw makeStringException(0, "Invalid AES key size, must be 128, 192 or 256 bit");
    }
}

namespace cryptohelper
{

// NB: static random IV used for AES, custom IV can be supplied to cryptohelper::aes* routines
static const char staticAesIV[16+1] = "j5P2Sz&DnW'FOW^{";

size32_t aesEncrypt(MemoryBuffer &out, size32_t inSz, const void *inBytes, size32_t keyLen, const char *key, const char iv[aesBlockSize])
{
    if (0 == inSz)
        return 0;
    OwnedEVPCipherCtx ctx(EVP_CIPHER_CTX_new());
    if (!ctx)
        throw makeEVPException(0, "Failed EVP_CIPHER_CTX_new");
    /* Initialise the encryption operation. IMPORTANT - ensure you use a key
     * and IV size appropriate for your cipher
     * In this example we are using 256 bit AES (i.e. a 256 bit key). The
     * IV size for *most* modes is the same as the block size. For AES this
     * is 128 bits
     * */
    if (!iv) iv = staticAesIV;
    if (1 != EVP_EncryptInit_ex(ctx, getAesCipher(keyLen), nullptr, (const unsigned char *)key, (const unsigned char *)iv))
        throw makeEVPException(0, "Failed EVP_EncryptInit_ex");

    /* Provide the message to be encrypted, and obtain the encrypted output.
     * EVP_EncryptUpdate can be called multiple times if necessary
     */

    const size32_t cipherBlockSz = 128;
    size32_t outMaxSz = inSz + cipherBlockSz/8;
    size32_t startSz = out.length();
    byte *outPtr = (byte *)out.reserveTruncate(outMaxSz);
    int outSz;
    if (1 != EVP_EncryptUpdate(ctx, (unsigned char *)outPtr, &outSz, (unsigned char *)inBytes, inSz))
        throw makeEVPException(0, "Failed EVP_EncryptUpdate");
    int ciphertext_len = outSz;

    /* Finalise the encryption. Further ciphertext bytes may be written at
     * this stage.
     */
    if (1 != EVP_EncryptFinal_ex(ctx, outPtr + outSz, &outSz))
        throw makeEVPException(0, "Failed EVP_EncryptFinal_ex");
    ciphertext_len += outSz;
    out.setLength(startSz+ciphertext_len); // truncate length of 'out' to final size
    return (size32_t)ciphertext_len;
}

size32_t aesDecrypt(MemoryBuffer &out, size32_t inSz, const void *inBytes, size32_t keyLen, const char *key, const char *iv)
{
    if (0 == inSz)
        return 0;
    OwnedEVPCipherCtx ctx(EVP_CIPHER_CTX_new());
    if (!ctx)
        throw makeEVPException(0, "Failed EVP_CIPHER_CTX_new");

    const size32_t cipherBlockSz = 128;
    // from man page - "should have sufficient room for (inl + cipher_block_size) bytes unless the cipher block size is 1 in which case inl bytes is sufficient"
    size32_t outMaxSz = (cipherBlockSz==1) ? inSz : (inSz + cipherBlockSz/8);
    size32_t startSz = out.length();
    byte *outPtr = (byte *)out.reserveTruncate(outMaxSz);

    /* Initialise the decryption operation. IMPORTANT - ensure you use a key
     * and IV size appropriate for your cipher
     * In this example we are using 256 bit AES (i.e. a 256 bit key). The
     * IV size for *most* modes is the same as the block size. For AES this
     * is 128 bits
     * */
    if (!iv) iv = staticAesIV;
    if (1 != EVP_DecryptInit_ex(ctx, getAesCipher(keyLen), nullptr, (const unsigned char *)key, (const unsigned char *)iv))
        throw makeEVPException(0, "Failed EVP_DecryptInit_ex");

    /* Provide the message to be decrypted, and obtain the plaintext output.
     * EVP_DecryptUpdate can be called multiple times if necessary
     */
    int outSz;
    if (1 != EVP_DecryptUpdate(ctx, outPtr, &outSz, (const unsigned char *)inBytes, inSz))
        throw makeEVPException(0, "Failed EVP_DecryptUpdate");
    int plaintext_len = outSz;

    /* Finalise the decryption. Further plaintext bytes may be written at
     * this stage.
     */
    if (1 != EVP_DecryptFinal_ex(ctx, outPtr + outSz, &outSz))
        throw makeEVPException(0, "Failed EVP_DecryptFinal_ex");

    plaintext_len += outSz;
    out.setLength(startSz+plaintext_len); // truncate length of 'out' to final size
    return (size32_t)plaintext_len;
}

size32_t aesEncryptWithRSAEncryptedKey(MemoryBuffer &out, size32_t inSz, const void *inBytes, const CLoadedKey &publicKey)
{
    // create random AES key and IV
    char randomAesKey[aesMaxKeySize];
    char randomIV[aesBlockSize];
    fillRandomData(aesMaxKeySize, randomAesKey);
    fillRandomData(aesBlockSize, randomIV);

    size32_t startSz = out.length();
    DelayedSizeMarker mark(out);
    publicKeyEncrypt(out, aesMaxKeySize, randomAesKey, publicKey);
    mark.write();
    out.append(aesBlockSize, randomIV);

    DelayedSizeMarker aesSz(out);
    aesEncrypt(out, inSz, inBytes, aesMaxKeySize, randomAesKey, randomIV);
    aesSz.write();
    return out.length()-startSz;
}

size32_t aesDecryptWithRSAEncryptedKey(MemoryBuffer &out, size32_t inSz, const void *inBytes, const CLoadedKey &privateKey)
{
    MemoryBuffer in;
    in.setBuffer(inSz, (void *)inBytes, false);
    // read encrypted AES key
    size32_t encryptedAESKeySz;
    in.read(encryptedAESKeySz);
    MemoryBuffer aesKey;
    size32_t decryptedAesKeySz = privateKeyDecrypt(aesKey, encryptedAESKeySz, in.readDirect(encryptedAESKeySz), privateKey);
    if (decryptedAesKeySz != aesMaxKeySize)
        throw makeStringException(0, "aesDecryptWithRSAEncryptedKey - invalid input");

    unsigned iVPos = in.getPos(); // read directly further down
    in.skip(aesBlockSize);

    size32_t aesEncryptedSz;
    in.read(aesEncryptedSz);

    return aesDecrypt(out, aesEncryptedSz, in.readDirect(aesEncryptedSz), aesMaxKeySize, (const char *)aesKey.bytes(), (const char *)in.bytes()+iVPos);
}


} // end of namespace cryptohelper

#endif // end of #if defined(_USE_OPENSSL) && !defined(_WIN32)

