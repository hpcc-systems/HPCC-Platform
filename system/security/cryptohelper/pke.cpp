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
 * Module dealing with openssl asymmetric key encryption/decription.
 * For now just RSA.
 */

#if defined(_USE_OPENSSL)

#include "jliball.hpp"
#include <openssl/pem.h>
#include <openssl/err.h>
#include <openssl/evp.h>

#include "cryptocommon.hpp"
#include "pke.hpp"

namespace cryptohelper
{

void CLoadedKey::loadKeyBio(size32_t keyLen, const char *keyMem)
{
#if defined(OPENSSL_VERSION_NUMBER) && OPENSSL_VERSION_NUMBER <= 0x10100000L
// openssl <= 1.0.1 prototyped BIO_new_mem_buf with void* even though it guaranteed it was const
    keyBio.setown(BIO_new_mem_buf((void *)keyMem, keyLen));
#else
    keyBio.setown(BIO_new_mem_buf(keyMem, keyLen));
#endif
    if (!keyBio)
        throwEVPException(0, "loadKeyBio: Failed to create bio for key");
}

void CLoadedKey::loadKeyFromMem(const char *key)
{
    size32_t keyLen = strlen(key);
    void *keyMem = keyMb.reserveTruncate(keyLen);
    memcpy(keyMem, key, keyLen);
    loadKeyBio(keyMb.length(), (const char *)keyMb.bytes());
}

bool CLoadedKey::loadKeyFromFile(const char *keyFile)
{
    OwnedIFile iFile = createIFile(keyFile);
    OwnedIFileIO iFileIO = iFile->open(IFOread);
    if (!iFileIO)
        return false;
    size32_t sz = iFile->size();
    verifyex(read(iFileIO, 0, sz, keyMb) == sz);
    loadKeyBio(keyMb.length(), (const char *)keyMb.bytes());
    return true;
}

void CLoadedKey::finalize(RSA *_rsa, const char *_keyName)
{
    rsa.setown(_rsa);
    key.setown(EVP_PKEY_new());
    EVP_PKEY_set1_RSA(key, _rsa);
    keyName.set(_keyName);
}

//called during PEM_read_bio_RSA_PUBKEY to set passphrase
int passphraseCB(char *passPhraseBuf, int passPhraseBufSize, int rwflag, void *pPassPhraseMB)
{
    size32_t len = ((MemoryBuffer*)pPassPhraseMB)->length();
    if (passPhraseBufSize >= (int)len)
    {
        memcpy(passPhraseBuf, ((MemoryBuffer*)pPassPhraseMB)->bufferBase(), len);
        return len;
    }
    PROGLOG("Private Key Passphrase too long (%d bytes), max %d", len, passPhraseBufSize);
    return 0;
}

class CLoadedPublicKeyFromFile : public CLoadedKey
{
public:
    CLoadedPublicKeyFromFile(const char *keyFile)
    {
        if (!loadKeyFromFile(keyFile))
            throwEVPExceptionV(0, "CLoadedPublicKeyFromFile: failed to open key: %s", keyFile);
        RSA *rsaKey = PEM_read_bio_RSA_PUBKEY(keyBio, nullptr, nullptr, nullptr);
        if (!rsaKey)
            throwEVPExceptionV(0, "Failed to create public key: %s", keyFile);
        finalize(rsaKey, keyFile);
    }
};

class CLoadedPublicKeyFromMemory : public CLoadedKey
{
public:
    CLoadedPublicKeyFromMemory(const char *key)
    {
        loadKeyFromMem(key);
        RSA *rsaKey = PEM_read_bio_RSA_PUBKEY(keyBio, nullptr, nullptr, nullptr);
        if (!rsaKey)
            throwEVPException(0, "Failed to create public key");
        finalize(rsaKey, "<inline>");
    }
};

class CLoadedPrivateKeyFromFile : public CLoadedKey
{
public:
    CLoadedPrivateKeyFromFile(const char *keyFile, size32_t passPhraseLen, const void *passPhrase)
    {
        if (!loadKeyFromFile(keyFile))
            throwEVPException(0, "CLoadedPrivateKeyFromFile: failed to open private key");
        MemoryBuffer passPhraseMB;
        passPhraseMB.setBuffer(passPhraseLen, (void *)passPhrase);
        RSA *rsaKey = PEM_read_bio_RSAPrivateKey(keyBio, nullptr, passphraseCB, (void *)&passPhraseMB);//Binary passphrase
        if (!rsaKey)
            throwEVPException(0, "Failed to create private key");
        finalize(rsaKey, keyFile);
    }
    CLoadedPrivateKeyFromFile(const char *keyFile, const char *passPhrase)
    {
        if (!loadKeyFromFile(keyFile))
            throwEVPException(0, "CLoadedPrivateKeyFromFile: failed to open private key");
        RSA *rsaKey = PEM_read_bio_RSAPrivateKey(keyBio, nullptr, nullptr, (void*)passPhrase);//plain text or no passphrase
        if (!rsaKey)
            throwEVPException(0, "Failed to create private key");
        finalize(rsaKey, keyFile);
    }
};

class CLoadedPrivateKeyFromMemory : public CLoadedKey
{
public:
    CLoadedPrivateKeyFromMemory(const char *key, size32_t passPhraseLen, const void *passPhrase)
    {
        MemoryBuffer passPhraseMB;
        passPhraseMB.setBuffer(passPhraseLen, (void *)passPhrase);
        loadKeyFromMem(key);
        RSA *rsaKey = PEM_read_bio_RSAPrivateKey(keyBio, nullptr, passphraseCB, (void *)&passPhraseMB);//Binary passphrase
        if (!rsaKey)
            throwEVPException(0, "Failed to create private key");
        finalize(rsaKey, "<inline>");
    }
    CLoadedPrivateKeyFromMemory(const char *key, const char *passPhrase)
    {
        loadKeyFromMem(key);
        RSA *rsaKey = PEM_read_bio_RSAPrivateKey(keyBio, nullptr, nullptr, (void*)passPhrase);//plain text or no passphrase
        if (!rsaKey)
            throwEVPException(0, "Failed to create private key");
        finalize(rsaKey, "<inline>");
    }
};

CLoadedKey *loadPublicKeyFromFile(const char *keyFile)
{
    return new CLoadedPublicKeyFromFile(keyFile);
}

CLoadedKey *loadPublicKeyFromMemory(const char *key)
{
    return new CLoadedPublicKeyFromMemory(key);
}

CLoadedKey *loadPrivateKeyFromFile(const char *keyFile, const char *passPhrase)
{
    return new CLoadedPrivateKeyFromFile(keyFile, passPhrase);
}

CLoadedKey *loadPrivateKeyFromMemory(const char *key, const char *passPhrase)
{
    return new CLoadedPrivateKeyFromMemory(key, passPhrase);
}

CLoadedKey *loadPrivateKeyFromFile(const char *keyFile, size32_t passPhraseLen, const void *passPhrase)
{
    return new CLoadedPrivateKeyFromFile(keyFile, passPhraseLen, passPhrase);
}

CLoadedKey *loadPrivateKeyFromMemory(const char *key, size32_t passPhraseLen, const void *passPhrase)
{
    return new CLoadedPrivateKeyFromMemory(key, passPhraseLen, passPhrase);
}

size32_t _publicKeyEncrypt(OwnedEVPMemory &dstMem, size32_t dstMaxSz, size32_t inSz, const void *inBytes, const CLoadedKey &publicKey)
{
    OwnedEVPPkeyCtx ctx(EVP_PKEY_CTX_new(publicKey, nullptr));
    if (!ctx || (EVP_PKEY_encrypt_init(ctx) <= 0) || (EVP_PKEY_CTX_set_rsa_padding(ctx, RSA_PKCS1_PADDING)) <= 0)
        throwEVPExceptionV(0, "publicKeyEncrypt: failed to initialize key: %s", publicKey.queryKeyName());

    /* Determine buffer length */
    size_t outLen;
    if (EVP_PKEY_encrypt(ctx, nullptr, &outLen, (unsigned char *)inBytes, inSz) <= 0)
        throwEVPExceptionV(0, "publicKeyEncrypt: [EVP_PKEY_encrypt] failed to encrypt with key: %s", publicKey.queryKeyName());

    if (dstMaxSz && outLen > dstMaxSz)
        return 0;
    void *dst = OPENSSL_malloc(outLen);
    if (!dst)
        throwEVPExceptionV(0, "publicKeyEncrypt: [OPENSSL_malloc] failed with key: %s", publicKey.queryKeyName());

    if (EVP_PKEY_encrypt(ctx, (unsigned char *)dst, &outLen, (unsigned char *)inBytes, inSz) <= 0)
        throwEVPExceptionV(0, "publicKeyEncrypt: [EVP_PKEY_encrypt] failed to encrypt with key: %s", publicKey.queryKeyName());

    dstMem.setown(dst);
    return (size32_t)outLen;
}

size32_t publicKeyEncrypt(void *dst, size32_t dstMaxSz, size32_t inSz, const void *inBytes, const CLoadedKey &publicKey)
{
    OwnedEVPMemory encrypted;
    size32_t encryptedSz = _publicKeyEncrypt(encrypted, dstMaxSz, inSz, inBytes, publicKey);
    if (encryptedSz)
        memcpy(dst, encrypted.get(), encryptedSz);
    return encryptedSz;
}

size32_t publicKeyEncrypt(MemoryBuffer &out, size32_t inSz, const void *inBytes, const CLoadedKey &publicKey)
{
    OwnedEVPMemory encrypted;
    size32_t encryptedSz = _publicKeyEncrypt(encrypted, 0, inSz, inBytes, publicKey);
    out.append(encryptedSz, encrypted);
    return encryptedSz;
}

size32_t _privateKeyDecrypt(OwnedEVPMemory &dstMem, size32_t dstMaxSz, size32_t inSz, const void *inBytes, const CLoadedKey &privateKey)
{
    OwnedEVPPkeyCtx ctx(EVP_PKEY_CTX_new(privateKey, nullptr));
    if (!ctx || (EVP_PKEY_decrypt_init(ctx) <= 0) || (EVP_PKEY_CTX_set_rsa_padding(ctx, RSA_PKCS1_PADDING)) <= 0)
        throwEVPException(0, "privateKeyDecrypt: [EVP_PKEY_decrypt_init] failed");

    /* Determine buffer length */
    size_t outLen;
    if (EVP_PKEY_decrypt(ctx, nullptr, &outLen, (const unsigned char *)inBytes, inSz) <= 0)
        throwEVPException(0, "privateKeyDecrypt: [EVP_PKEY_decrypt] failed to decrypt");

    if (dstMaxSz && outLen > dstMaxSz)
        return 0;
    void *dst = OPENSSL_malloc(outLen);
    if (!dst)
        throwEVPException(0, "privateKeyDecrypt: [OPENSSL_malloc] failed");

    if (EVP_PKEY_decrypt(ctx, (unsigned char *)dst, &outLen, (const unsigned char *)inBytes, inSz) <= 0)
        throwEVPException(0, "privateKeyDecrypt: [EVP_PKEY_decrypt] failed to decrypt");

    dstMem.setown(dst);
    return (size32_t)outLen;
}

size32_t privateKeyDecrypt(void *dst, size32_t dstMaxSz, size32_t inSz, const void *inBytes, const CLoadedKey &privateKey)
{
    OwnedEVPMemory decrypted;
    size32_t decryptedSz = _privateKeyDecrypt(decrypted, dstMaxSz, inSz, inBytes, privateKey);
    if (decryptedSz)
        memcpy(dst, decrypted.get(), decryptedSz);
    return decryptedSz;
}

size32_t privateKeyDecrypt(MemoryBuffer &out, size32_t inSz, const void *inBytes, const CLoadedKey &privateKey)
{
    OwnedEVPMemory decrypted;
    size32_t decryptedSz = _privateKeyDecrypt(decrypted, 0, inSz, inBytes, privateKey);
    out.append(decryptedSz, decrypted);
    return decryptedSz;
}

} // end of namespace cryptohelper

#endif // end of #if defined(_USE_OPENSSL)

