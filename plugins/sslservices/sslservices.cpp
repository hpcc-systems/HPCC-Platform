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

#include "sslservices.hpp"

#include "openssl/err.h"
#include "openssl/evp.h"
#include "openssl/pem.h"

#include "jlog.hpp"

#include <string>
#include <memory>
#include <vector>

#define CURRENT_SSLSERVICES_VERSION "sslservices plugin 1.0.0"

static const char* sslservicesCompatibleVersions[] = {
    CURRENT_SSLSERVICES_VERSION,
    NULL };

SSLSERVICES_API bool getECLPluginDefinition(ECLPluginDefinitionBlock* pb)
{
    if (pb->size == sizeof(ECLPluginDefinitionBlockEx))
    {
        ECLPluginDefinitionBlockEx* pbx = static_cast<ECLPluginDefinitionBlockEx*>(pb);
        pbx->compatibleVersions = sslservicesCompatibleVersions;
    }
    else if (pb->size != sizeof(ECLPluginDefinitionBlock))
    {
        return false;
    }

    pb->magicVersion = PLUGIN_VERSION;
    pb->version = CURRENT_SSLSERVICES_VERSION;
    pb->moduleName = "sslservices";
    pb->ECL = NULL;
    pb->flags = PLUGIN_IMPLICIT_MODULE;
    pb->description = "ECL plugin library for the C++ API in OpenSSL";

    return true;
}

namespace nsSSLServices
{

void failOpenSSLError(const std::string& context)
{
    char   buffer[120];

    ERR_error_string_n(ERR_get_error(), buffer, sizeof(buffer));

    std::string desc = "Error within ";
    desc += context;
    desc += ": ";
    desc += buffer;

    rtlFail(-1, desc.c_str());
}

//called during PEM_read_bio_PrivateKey to set passphrase
int passphraseCB(char *passPhraseBuf, int passPhraseBufSize, int rwflag, void *pPassPhraseMB)
{
    size32_t len = ((MemoryBuffer*)pPassPhraseMB)->length();
    if (((size32_t)passPhraseBufSize) >= len)
    {
        memcpy(passPhraseBuf, ((MemoryBuffer*)pPassPhraseMB)->bufferBase(), len);
        return len;
    }
    PROGLOG("Private Key Passphrase too long (%d bytes), max %d", len, passPhraseBufSize);
    return 0;
}

bool isPublicKey(size32_t keyLen, const char * key)
{
    for (size32_t i = 0; key[i] != '\n' && i < (keyLen - 15); i++)
    {
        if (key[i] == 'P')
        {
            if (strncmp(key + i + 1, "UBLIC KEY-----", 14) == 0)
                return true;
        }
    }
    return false;
}

static constexpr size32_t SSLSERVICES_MAX_CACHE_SIZE = 10;
static constexpr bool PRINT_STATS = false;

/**
 * Simple cache that is used for both ciphers and digests
 *
 * NOTE: Should only be used with the thread-local storage class
 * specifier since the results cannot be relied on if called from
 * multiple threads.
*/
template <typename T>
class AlgorithmCache
{
public:
    AlgorithmCache()
    {
        setCacheName();
    }

    ~AlgorithmCache()
    {
        if (PRINT_STATS)
            LOG(MCmonitorMetric, "{ \"type\": \"metric\", \"name\": \"sslServiceCache%s\", \"hits\": \"%u\", \"misses\": \"%u\" }", cacheName.c_str(), hits, misses);
        cache.clear();
    }

    const T * checkCache(const char * algorithm_name)
    {
        for (auto& c : cache)
        {
            if (std::get<0>(c) == algorithm_name)
            {
                hits++;
                return std::get<1>(c);
            }
        }
        misses++;
        const T * newObj = getObjectByName(algorithm_name);
        if (newObj)
        {
            cache.emplace_front(algorithm_name, newObj);
            if (cache.size() > SSLSERVICES_MAX_CACHE_SIZE)
                cache.pop_back();
        }
        else
            failOpenSSLError("adding new object to cache");

        return newObj;
    }

private:
    unsigned hits = 0;
    unsigned misses = 0;
    std::string cacheName;
    std::list<std::tuple<std::string, const T *>> cache;

    void setCacheName();
    const T * getObjectByName(const char * name);
};

template <>
void AlgorithmCache<EVP_CIPHER>::setCacheName() {cacheName = "Cipher";}

template <>
void AlgorithmCache<EVP_MD>::setCacheName() {cacheName = "Digest";}

template <>
const EVP_CIPHER * AlgorithmCache<EVP_CIPHER>::getObjectByName(const char * name) { return EVP_get_cipherbyname(name); }

template <>
const EVP_MD * AlgorithmCache<EVP_MD>::getObjectByName(const char * name) { return EVP_get_digestbyname(name); }

/**
 * Simple cache that is used for both public and private keys.
 *
 * NOTE: Should only be used with the thread-local storage class
 * specifier since the results cannot be relied on if called from
 * multiple threads.
*/
class PKeyCache
{
public:
    ~PKeyCache()
    {
        if (PRINT_STATS)
            LOG(MCmonitorMetric, "{ \"type\": \"metric\", \"name\": \"sslServiceCachePKey\", \"hits\": \"%u\", \"misses\": \"%u\" }", hits, misses);
        cache.clear();
    }

    std::shared_ptr<EVP_PKEY> checkCache(size32_t keyLen, const char * key, size32_t passphraseLen, const void * passphrase)
    {
        hash64_t pkeyHash = hashc_fnv1a(static_cast<const byte *>(passphrase), passphraseLen, hashc_fnv1a(reinterpret_cast<const byte *>(key), keyLen, fnvInitialHash32));
        for (auto& c : cache)
        {
            if (pkeyHash == std::get<0>(c))
            {
                hits++;
                return std::get<1>(c);
            }
        }

        misses++;

        BIO * bio = BIO_new_mem_buf(key, keyLen);
        if (!bio)
            failOpenSSLError("creating buffer for EVP_PKEY");

        std::shared_ptr<EVP_PKEY> pkey;
        if (isPublicKey(keyLen, key))
            pkey.reset(PEM_read_bio_PUBKEY(bio, nullptr, nullptr, nullptr), EVP_PKEY_free);
        else
        {
            MemoryBuffer passphraseMB;
            passphraseMB.setBuffer(passphraseLen, (void *)passphrase);
            pkey.reset(PEM_read_bio_PrivateKey(bio, nullptr, passphraseCB, static_cast<void *>(&passphraseMB)), EVP_PKEY_free);
        }
        BIO_free(bio);

        if (pkey)
        {
            cache.emplace_front(pkeyHash, pkey);
            if (cache.size() > SSLSERVICES_MAX_CACHE_SIZE)
                cache.pop_back();
        }
        else
            failOpenSSLError("loading a pkey");

        return pkey;
    }

private:
    unsigned hits = 0;
    unsigned misses = 0;
    std::list<std::tuple<hash64_t, std::shared_ptr<EVP_PKEY>>> cache;
};


static thread_local PKeyCache pkeyCache;

static thread_local AlgorithmCache<EVP_CIPHER> cipherCache;
static thread_local AlgorithmCache<EVP_MD> digestCache;
} // nsSSLServices

using namespace nsSSLServices;

//--------------------------------------------------------------------------
// Advertised Entry Point Functions
//--------------------------------------------------------------------------

SSLSERVICES_API void SSLSERVICES_CALL digestAvailableAlgorithms(ICodeContext *ctx, size32_t & __lenResult, void * & __result)
{
    // Get all the hash (digest) names
    std::vector<std::string> digestNames;
    EVP_MD_do_all([](const EVP_MD * md, const char * name, const char * description, void * arg) {
        std::vector<std::string> * digestNames = static_cast<std::vector<std::string>*>(arg);
        digestNames->push_back(name);
    }, &digestNames);
    EVP_cleanup();

    __lenResult = 0;
    __result = nullptr;

    // Determine final size of returned dataset
    for (auto& name : digestNames)
        __lenResult += (sizeof(size32_t) + name.size());

    // Allocate and populate result block
    if (__lenResult > 0)
    {
        MemoryBuffer resultBuffer(__lenResult);
        size32_t stringSize = 0;
        for (auto& name : digestNames)
        {
            stringSize = name.size();
            resultBuffer.append(stringSize);
            resultBuffer.append(stringSize, name.data());
        }

        __result = resultBuffer.detachOwn();
    }
}

SSLSERVICES_API void SSLSERVICES_CALL digestHash(ICodeContext *ctx, size32_t & __lenResult, void * & __result, size32_t len_indata, const void * _indata, const char * _algorithm_name)
{
    if (strlen(_algorithm_name) == 0)
        rtlFail(-1, "No hash digest name provided");

    const EVP_MD * md = digestCache.checkCache(_algorithm_name);

    EVP_MD_CTX * mdContext = EVP_MD_CTX_new();
    if (!mdContext)
        failOpenSSLError("creating a hash digest context");

    try
    {
        if (EVP_DigestInit_ex(mdContext, md, nullptr) != 1)
            failOpenSSLError("EVP_DigestInit_ex");

        if (EVP_DigestUpdate(mdContext, _indata, len_indata) != 1)
            failOpenSSLError("EVP_DigestUpdate");

        __lenResult = EVP_MD_CTX_get_size(mdContext);
        MemoryBuffer resultBuffer(__lenResult);

        if (EVP_DigestFinal_ex(mdContext, static_cast<byte *>(resultBuffer.bufferBase()), nullptr) != 1)
            failOpenSSLError("EVP_DigestFinal_ex");

        __result = resultBuffer.detachOwn();
    }
    catch (...)
    {
        EVP_MD_CTX_free(mdContext);
        throw;
    }

    EVP_MD_CTX_free(mdContext);
}

// Symmetric ciphers

SSLSERVICES_API void SSLSERVICES_CALL cipherAvailableAlgorithms(ICodeContext *ctx, size32_t & __lenResult, void * & __result)
{
    // Get all the cipher names
    std::vector<std::string> cipherNames;
    EVP_CIPHER_do_all([](const EVP_CIPHER * cipher, const char * from, const char * to, void * x) {
        auto cipherNames = static_cast<std::vector<std::string> *>(x);
        cipherNames->push_back(from);
    }, &cipherNames);
    EVP_cleanup();

    __lenResult = 0;
    __result = nullptr;

    // Determine final size of returned dataset
    for (auto& name : cipherNames)
        __lenResult += (sizeof(uint32_t) + name.size());

    // Allocate and populate result block
    if (__lenResult > 0)
    {
        MemoryBuffer resultBuffer(__lenResult);
        size32_t stringSize = 0;
        for (auto& name : cipherNames)
        {
            stringSize = name.size();
            resultBuffer.append(stringSize);
            resultBuffer.append(stringSize, name.data());
        }

        __result = resultBuffer.detachOwn();
    }
}

SSLSERVICES_API uint16_t SSLSERVICES_CALL cipherIVSize(ICodeContext *ctx, const char * algorithm_name)
{
    if (strlen(algorithm_name) == 0)
        rtlFail(-1, "No algorithm name provided");

    // Load the cipher
    const EVP_CIPHER * cipher = cipherCache.checkCache(algorithm_name);

    return static_cast<uint16_t>(EVP_CIPHER_iv_length(cipher));
}

SSLSERVICES_API void SSLSERVICES_CALL cipherEncrypt(ICodeContext *ctx, size32_t & __lenResult, void * & __result, size32_t len_plaintext, const void * _plaintext, const char * _algorithm_name, size32_t len_passphrase, const void * _passphrase, size32_t len_iv, const void * _iv, size32_t len_salt, const void * _salt)
{
    __result = nullptr;
    __lenResult = 0;

    bool hasIV = (len_iv > 0);
    bool hasSalt = (len_salt > 0);

    // Initial sanity check of our arguments
    if (strlen(_algorithm_name) == 0)
        rtlFail(-1, "No algorithm name provided");
    if (len_passphrase == 0)
        rtlFail(-1, "No passphrase provided");
    if (hasSalt && len_salt != 8)
        rtlFail(-1, "Salt value must be exactly 8 bytes in size");

    if (len_plaintext > 0)
    {
        // Load the cipher
        const EVP_CIPHER * cipher = cipherCache.checkCache(_algorithm_name);

        size32_t cipherIVSize = EVP_CIPHER_iv_length(cipher);
        if (hasIV && len_iv != static_cast<size32_t>(cipherIVSize))
            rtlFail(-1, "Supplied IV is an incorrect size");

        // Generate a key and an IV (if one was not provided)
        MemoryBuffer key(EVP_MAX_KEY_LENGTH);
        MemoryBuffer iv(cipherIVSize);
        if (!EVP_BytesToKey(cipher, EVP_sha256(), (hasSalt ? static_cast<const byte *>(_salt) : nullptr), static_cast<const byte *>(_passphrase), len_passphrase, 1, static_cast<byte *>(key.bufferBase()), static_cast<byte *>(iv.bufferBase())))
            failOpenSSLError("generating an encryption key from the passphrase");

        // If an IV was supplied, copy it over the one that was generated
        if (hasIV)
            iv.append(cipherIVSize, _iv);

        // Initialize the context
        EVP_CIPHER_CTX * encryptCtx = EVP_CIPHER_CTX_new();
        if (!encryptCtx)
            failOpenSSLError("EVP_CIPHER_CTX_new");

        // Reserve buffers
        __lenResult = len_plaintext + EVP_CIPHER_block_size(cipher); // max size, may be changed later
        MemoryBuffer resultBuffer(__lenResult);

        try
        {
            int len = 0;
            size32_t ciphertextLen = 0;

            if (EVP_EncryptInit_ex(encryptCtx, cipher, nullptr, static_cast<byte *>(key.bufferBase()),static_cast<byte *>(iv.bufferBase())) != 1)
                failOpenSSLError("EVP_EncryptInit_ex");

            if (EVP_EncryptUpdate(encryptCtx, static_cast<byte *>(resultBuffer.bufferBase()), &len, static_cast<const byte *>(_plaintext), len_plaintext) != 1)
                failOpenSSLError("EVP_EncryptUpdate");
            ciphertextLen = len;

            if (EVP_EncryptFinal_ex(encryptCtx, static_cast<byte *>(resultBuffer.bufferBase()) + len, &len) != 1)
                failOpenSSLError("EVP_EncryptFinal_ex");
            ciphertextLen += len;
            __lenResult = ciphertextLen;
            __result = resultBuffer.detachOwn();
        }
        catch (...)
        {
            EVP_CIPHER_CTX_free(encryptCtx);
            __lenResult = 0;
            rtlFree(__result);
            __result = nullptr;
            throw;
        }

        EVP_CIPHER_CTX_free(encryptCtx);
    }
}

SSLSERVICES_API void SSLSERVICES_CALL cipherDecrypt(ICodeContext *ctx, size32_t & __lenResult, void * & __result, size32_t len_ciphertext, const void * _ciphertext, const char * _algorithm_name, size32_t len_passphrase, const void * _passphrase, size32_t len_iv, const void * _iv, size32_t len_salt, const void * _salt)
{
    __result = nullptr;
    __lenResult = 0;

    bool hasIV = (len_iv > 0);
    bool hasSalt = (len_salt > 0);

    // Initial sanity check of our arguments
    if (strlen(_algorithm_name) == 0)
        rtlFail(-1, "No algorithm name provided");
    if (len_passphrase == 0)
        rtlFail(-1, "No passphrase provided");
    if (hasSalt && len_salt != 8)
        rtlFail(-1, "Salt value must be exactly 8 bytes in size");

    if (len_ciphertext > 0)
    {
        // Load the cipher
        const EVP_CIPHER * cipher = cipherCache.checkCache(_algorithm_name);

        size32_t cipherIVSize = EVP_CIPHER_iv_length(cipher);
        if (hasIV && len_iv != static_cast<size32_t>(cipherIVSize))
            rtlFail(-1, "Supplied IV is an incorrect size");

        // Generate a key and an IV (if one was not provided)
        MemoryBuffer key(EVP_MAX_KEY_LENGTH);
        MemoryBuffer iv(cipherIVSize);
        if (!EVP_BytesToKey(cipher, EVP_sha256(), (hasSalt ? static_cast<const byte *>(_salt) : nullptr), static_cast<const byte *>(_passphrase), len_passphrase, 1, static_cast<byte *>(key.bufferBase()), static_cast<byte *>(iv.bufferBase())))
            failOpenSSLError("generating an encryption key from the passphrase");

        // If an IV was supplied, copy it over the one that was generated
        if (hasIV)
            iv.append(cipherIVSize, _iv);

        // Initialize the context
        EVP_CIPHER_CTX * decryptCtx = EVP_CIPHER_CTX_new();
        if (!decryptCtx)
            failOpenSSLError("EVP_CIPHER_CTX_new");

        // Reserve buffers
        __lenResult = len_ciphertext; // max size, may be changed later
        MemoryBuffer resultBuffer(__lenResult);

        try
        {
            int len = 0;
            size32_t plaintextLen = 0;

            if (EVP_DecryptInit_ex(decryptCtx, cipher, nullptr, static_cast<byte *>(key.bufferBase()), static_cast<byte *>(iv.bufferBase())) != 1)
                failOpenSSLError("EVP_DecryptInit_ex");

            if (EVP_DecryptUpdate(decryptCtx, static_cast<byte *>(resultBuffer.bufferBase()), &len, static_cast<const byte *>(_ciphertext), len_ciphertext) != 1)
                failOpenSSLError("EVP_DecryptUpdate");
            plaintextLen = len;

            if (EVP_DecryptFinal_ex(decryptCtx, static_cast<byte *>(resultBuffer.bufferBase()) + len, &len) != 1)
                failOpenSSLError("EVP_DecryptFinal_ex");
            plaintextLen += len;
            __lenResult = plaintextLen;
            __result = resultBuffer.detachOwn();
        }
        catch (...)
        {
            EVP_CIPHER_CTX_free(decryptCtx);
            __lenResult = 0;
            rtlFree(__result);
            __result = nullptr;
            throw;
        }

        EVP_CIPHER_CTX_free(decryptCtx);
    }
}

// PublicKey functions

SSLSERVICES_API void SSLSERVICES_CALL pkRSASeal(ICodeContext *ctx, size32_t & __lenResult, void * & __result, size32_t len_plaintext, const void * _plaintext, bool isAll_pem_public_keys, size32_t len_pem_public_keys, const void * _pem_public_keys, const char * _algorithm_name)
{
    __result = nullptr;
    __lenResult = 0;

    // Initial sanity check of our arguments
    if (len_pem_public_keys == 0)
        rtlFail(-1, "No public keys provided");

    if (!isAll_pem_public_keys && len_plaintext > 0)
    {
        std::vector<std::shared_ptr<EVP_PKEY>> publicKeysSP;
        std::vector<EVP_PKEY *> publicKeys;
        EVP_CIPHER_CTX * encryptCtx = nullptr;
        byte ** encryptedKeys = nullptr;
        MemoryBuffer iv;
        MemoryBuffer ciphertext;

        try
        {
            // Parse public keys and stuff them into a vector
            const char * pubKeyPtr = static_cast<const char *>(_pem_public_keys);
            const char * endPtr = pubKeyPtr + len_pem_public_keys;
            while (pubKeyPtr < endPtr)
            {
                const size32_t keySize = *(reinterpret_cast<const size32_t *>(pubKeyPtr));
                pubKeyPtr += sizeof(keySize);
                publicKeysSP.push_back(pkeyCache.checkCache(keySize, pubKeyPtr, 0, nullptr));
                publicKeys.push_back(publicKeysSP.back().get());
                pubKeyPtr += keySize;
            }

            // Load the cipher
            const EVP_CIPHER * cipher = cipherCache.checkCache(_algorithm_name);

            // Allocate memory for encrypted keys
            size32_t keyCount = publicKeys.size();
            encryptedKeys = new byte *[keyCount];
            for (size32_t x = 0; x < keyCount; x++)
                encryptedKeys[x] = new byte [EVP_PKEY_size(publicKeys[x])];

            // Allocate memory for the IV
            size32_t ivLen = EVP_CIPHER_iv_length(cipher);
            iv.ensureCapacity(ivLen);

            // Allocate buffer for ciphertext
            size32_t ciphertextLen = len_plaintext + EVP_CIPHER_block_size(cipher);
            ciphertext.ensureCapacity(ciphertextLen);

            // Create and initialize the context
            encryptCtx = EVP_CIPHER_CTX_new();
            if (!encryptCtx)
                failOpenSSLError("creating cipher context");

            // Initialize the envelope
            std::vector<int> keyLens(keyCount);
            if (EVP_SealInit(encryptCtx, cipher, encryptedKeys, keyLens.data(), static_cast<byte *>(iv.bufferBase()), publicKeys.data(), keyCount) != static_cast<int>(keyCount))
                failOpenSSLError("EVP_SealInit");

            // Update the envelope (encrypt the plaintext)
            int len = 0;
            if (EVP_SealUpdate(encryptCtx, static_cast<byte *>(ciphertext.bufferBase()), &len, reinterpret_cast<const byte *>(_plaintext), len_plaintext) != 1)
                failOpenSSLError("EVP_SealUpdate");
            ciphertextLen = len;

            // Finalize the envelope's ciphertext
            if (EVP_SealFinal(encryptCtx, static_cast<byte *>(ciphertext.bufferBase()) + len, &len) != 1)
                failOpenSSLError("EVP_SealFinal");
            ciphertextLen += len;

            size32_t totalKeyLen = 0;
            for (size32_t i = 0; i < keyCount; i++)
                totalKeyLen += keyLens[i];

            // We need to prepend the ciphertext with metadata so the blob can be decrypted;
            // this is potentially nonstandard
            MemoryBuffer outBuffer;
            outBuffer.ensureCapacity(ivLen + (sizeof(size32_t)*(keyCount+1)) + totalKeyLen + ciphertextLen);
            // IV comes first; its length can be derived from the cipher
            outBuffer.append(ivLen, static_cast<byte *>(iv.bufferBase()));
            // Number of keys (size32_t)
            outBuffer.append(sizeof(keyCount), reinterpret_cast<byte *>(&keyCount));
            // Keys; each is (size32_t) + <content>
            for (size32_t x = 0; x < keyCount; x++)
            {
                size32_t keyLen = keyLens[x];
                outBuffer.append(sizeof(keyLen), reinterpret_cast<byte *>(&keyLen));
                outBuffer.append(keyLen, encryptedKeys[x]);
            }
            // And finally the ciphertext
            outBuffer.append(ciphertextLen, static_cast<byte *>(ciphertext.bufferBase()));

            // Copy to the ECL result buffer
            __lenResult = outBuffer.length();
            __result = outBuffer.detachOwn();

            // Cleanup
            EVP_CIPHER_CTX_free(encryptCtx);
            for (size_t i = 0; i < publicKeys.size(); i++)
                delete [] encryptedKeys[i];
            delete [] encryptedKeys;
        }
        catch (...)
        {
            if (encryptCtx)
                EVP_CIPHER_CTX_free(encryptCtx);
            for (size_t i = 0; i < publicKeys.size(); i++)
                delete [] encryptedKeys[i];
            delete [] encryptedKeys;
            __lenResult = 0;
            rtlFree(__result);
            __result = nullptr;
            throw;
        }
    }
}

SSLSERVICES_API void SSLSERVICES_CALL pkRSAUnseal(ICodeContext *ctx, size32_t & __lenResult, void * & __result, size32_t len_ciphertext, const void * _ciphertext, size32_t len_passphrase, const void * _passphrase, size32_t len_pem_private_key, const char * _pem_private_key, const char * _algorithm_name)
{
    __result = nullptr;
    __lenResult = 0;

    // Initial sanity check of our arguments
    if (len_pem_private_key == 0)
        rtlFail(-1, "No private key provided");

    if (len_ciphertext > 0)
    {
        EVP_CIPHER_CTX * decryptCtx = nullptr;
        MemoryBuffer symmetricKey;
        MemoryBuffer iv;
        MemoryBuffer plaintext;

        try
        {
            // Load the private key
            std::shared_ptr<EVP_PKEY> privateKey = pkeyCache.checkCache(len_pem_private_key, _pem_private_key, len_passphrase, _passphrase);

            // Load the cipher
            const EVP_CIPHER * cipher = cipherCache.checkCache(_algorithm_name);

            // Allocate memory for the symmetric key and IV
            size32_t keyLen = EVP_PKEY_size(privateKey.get());
            symmetricKey.ensureCapacity(keyLen);
            size32_t ivLen = EVP_CIPHER_iv_length(cipher);
            iv.ensureCapacity(ivLen);

            // Unpack the structured ciphertext to extract the metadata
            const byte * inPtr = static_cast<const byte *>(_ciphertext);
            // IV comes first, length determined by the cipher
            iv.append(ivLen, inPtr);
            inPtr += ivLen;
            // Number of keys embedded in the metadata (size32_t)
            size32_t keyCount = 0;
            memcpy(&keyCount, inPtr, sizeof(keyCount));
            inPtr += sizeof(keyCount);
            // The keys; each has a length (size32_t) then contents
            std::vector<std::string> encryptedKeys;
            for (size32_t x = 0; x < keyCount; x++)
            {
                size32_t keySize = 0;
                memcpy(&keySize, inPtr, sizeof(keySize));
                inPtr += sizeof(keySize);
                encryptedKeys.emplace_back(reinterpret_cast<const char *>(inPtr), keySize);
                inPtr += keySize;
            }

            const byte * newCipherText = inPtr;
            size32_t newCipherTextLen = (len_ciphertext - (reinterpret_cast<const char *>(inPtr) - static_cast<const char *>(_ciphertext)));

            // Initialize the context for decryption
            decryptCtx = EVP_CIPHER_CTX_new();
            if (!decryptCtx)
                failOpenSSLError("creating cipher context");

            // Find an encrypted key that we can decrypt
            bool found = false;
            for (auto& encryptedKey : encryptedKeys)
            {
                if (EVP_OpenInit(decryptCtx, cipher, reinterpret_cast<const unsigned char *>(encryptedKey.data()), encryptedKey.size(), static_cast<byte *>(iv.bufferBase()), privateKey.get()) == 1)
                {
                    found = true;
                    break;
                }
            }
            if (!found)
                failOpenSSLError("EVP_OpenInit");

            // Allocate memory for the plaintext
            size32_t plaintextLen = newCipherTextLen;
            plaintext.ensureCapacity(plaintextLen);

            int len = 0;
            if (EVP_OpenUpdate(decryptCtx, static_cast<byte *>(plaintext.bufferBase()), &len, newCipherText, newCipherTextLen) != 1)
                failOpenSSLError("EVP_OpenUpdate");
            plaintextLen = len;

            if (EVP_OpenFinal(decryptCtx, static_cast<byte *>(plaintext.bufferBase()) + len, &len) != 1)
                failOpenSSLError("EVP_OpenFinal");
            plaintextLen += len;

            // Copy to the ECL result buffer
            __lenResult = plaintextLen;
            __result = plaintext.detachOwn();

            // Cleanup
            EVP_CIPHER_CTX_free(decryptCtx);
        }
        catch (...)
        {
            if (decryptCtx)
                EVP_CIPHER_CTX_free(decryptCtx);
            __lenResult = 0;
            rtlFree(__result);
            __result = nullptr;
            throw;
        }
    }
}

SSLSERVICES_API void SSLSERVICES_CALL pkEncrypt(ICodeContext *ctx, size32_t & __lenResult, void * & __result, size32_t len_plaintext, const void * _plaintext, size32_t len_pem_public_key, const char * _pem_public_key)
{
    __result = nullptr;
    __lenResult = 0;

    // Initial sanity check of our arguments
    if (len_pem_public_key == 0)
        rtlFail(-1, "No public key provided");

    EVP_PKEY_CTX * encryptCtx = nullptr;

    if (len_plaintext > 0)
    {
        try
        {
            // Load key from buffer
            std::shared_ptr<EVP_PKEY> publicKey = pkeyCache.checkCache(len_pem_public_key, _pem_public_key, 0, nullptr);

            // Create encryption context
            encryptCtx = EVP_PKEY_CTX_new(publicKey.get(), nullptr);
            if (!encryptCtx)
                failOpenSSLError("publicKey");
            if (EVP_PKEY_encrypt_init(encryptCtx) <= 0)
                failOpenSSLError("EVP_PKEY_encrypt_init");

            // Determine max size of output
            size_t outLen = 0;
            if (EVP_PKEY_encrypt(encryptCtx, nullptr, &outLen, reinterpret_cast<const byte *>(_plaintext), len_plaintext) <= 0)
                failOpenSSLError("EVP_PKEY_encrypt");

            // Set actual size of output
            MemoryBuffer resultBuffer(outLen);

            if (EVP_PKEY_encrypt(encryptCtx, static_cast<byte *>(resultBuffer.bufferBase()), &outLen, reinterpret_cast<const byte *>(_plaintext), len_plaintext) <= 0)
                failOpenSSLError("EVP_PKEY_encrypt");

            __lenResult = outLen;
            __result = resultBuffer.detachOwn();

            EVP_PKEY_CTX_free(encryptCtx);
        }
        catch (...)
        {
            if (encryptCtx)
                EVP_PKEY_CTX_free(encryptCtx);
            rtlFree(__result);
            __lenResult = 0;
            throw;
        }
    }
}

SSLSERVICES_API void SSLSERVICES_CALL pkDecrypt(ICodeContext *ctx, size32_t & __lenResult, void * & __result, size32_t len_ciphertext, const void * _ciphertext, size32_t len_passphrase, const void * _passphrase, size32_t len_pem_private_key, const char * _pem_private_key)
{
    __result = nullptr;
    __lenResult = 0;

    // Initial sanity check of our arguments
    if (len_pem_private_key == 0)
        rtlFail(-1, "No private key provided");

    EVP_PKEY_CTX * decryptCtx = nullptr;

    if (len_ciphertext > 0)
    {
        try
        {
            // Load key from buffer
            std::shared_ptr<EVP_PKEY> privateKey = pkeyCache.checkCache(len_pem_private_key, _pem_private_key, len_passphrase, _passphrase);

            // Create decryption context
            decryptCtx = EVP_PKEY_CTX_new(privateKey.get(), nullptr);
            if (!decryptCtx)
                failOpenSSLError("EVP_PKEY_CTX_new");
            if (EVP_PKEY_decrypt_init(decryptCtx) <= 0)
                failOpenSSLError("EVP_PKEY_decrypt_init");

            // Determine max size of output
            size_t outLen = 0;
            if (EVP_PKEY_decrypt(decryptCtx, nullptr, &outLen, reinterpret_cast<const byte *>(_ciphertext), len_ciphertext) <= 0)
                failOpenSSLError("EVP_PKEY_decrypt");

            // Set actual size of output
            MemoryBuffer resultBuffer(outLen);

            if (EVP_PKEY_decrypt(decryptCtx, static_cast<byte *>(resultBuffer.bufferBase()), &outLen, reinterpret_cast<const byte *>(_ciphertext), len_ciphertext) <= 0)
                failOpenSSLError("EVP_PKEY_decrypt");

            __lenResult = outLen;
            __result = resultBuffer.detachOwn();

            EVP_PKEY_CTX_free(decryptCtx);
        }
        catch (...)
        {
            if (decryptCtx)
                EVP_PKEY_CTX_free(decryptCtx);
            rtlFree(__result);
            __lenResult = 0;
            throw;
        }
    }
}

SSLSERVICES_API void SSLSERVICES_CALL pkSign(ICodeContext *ctx, size32_t & __lenResult, void * & __result, size32_t len_plaintext, const void * _plaintext, size32_t len_passphrase, const void * _passphrase, size32_t len_pem_private_key, const char * _pem_private_key, const char * _algorithm_name)
{
    EVP_MD_CTX *mdCtx = nullptr;
    __result = nullptr;
    __lenResult = 0;

    try
    {
        // Load the private key from the PEM string
        std::shared_ptr<EVP_PKEY> privateKey = pkeyCache.checkCache(len_pem_private_key, _pem_private_key, len_passphrase, _passphrase);

        // Create and initialize the message digest context
        mdCtx = EVP_MD_CTX_new();
        if (!mdCtx)
            failOpenSSLError("EVP_MD_CTX_new (pkSign)");

        const EVP_MD *md = digestCache.checkCache(_algorithm_name);

        if (EVP_DigestSignInit(mdCtx, nullptr, md, nullptr, privateKey.get()) <= 0)
            failOpenSSLError("EVP_DigestSignInit (pkSign)");

        // Add plaintext to context
        if (EVP_DigestSignUpdate(mdCtx, _plaintext, len_plaintext) <= 0)
            failOpenSSLError("EVP_DigestSignUpdate (pkSign)");

        // Determine the buffer length for the signature
        size_t signatureLen = 0;
        if (EVP_DigestSignFinal(mdCtx, nullptr, &signatureLen) <= 0)
            failOpenSSLError("determining result length (pkSign)");

        // Allocate memory for the signature
        MemoryBuffer signatureBuffer(signatureLen);

        // Perform the actual signing
        if (EVP_DigestSignFinal(mdCtx, static_cast<byte *>(signatureBuffer.bufferBase()), &signatureLen) <= 0)
            failOpenSSLError("signing (pkSign)");

        // Set the result
        __lenResult = signatureLen;
        __result = signatureBuffer.detachOwn();

        // Clean up
        EVP_MD_CTX_free(mdCtx);
    }
    catch (...)
    {
        if (mdCtx)
            EVP_MD_CTX_free(mdCtx);
        rtlFree(__result);
        __lenResult = 0;
        throw;
    }
}

SSLSERVICES_API bool SSLSERVICES_CALL pkVerifySignature(ICodeContext *ctx, size32_t len_signature, const void * _signature, size32_t len_signedData, const void * _signedData, size32_t len_pem_public_key, const char * _pem_public_key, const char * _algorithm_name)
{
    EVP_MD_CTX *mdCtx = nullptr;

    try
    {
        // Load the public key from the PEM string
        std::shared_ptr<EVP_PKEY> publicKey = pkeyCache.checkCache(len_pem_public_key, _pem_public_key, 0, nullptr);

        // Create and initialize the message digest context
        mdCtx = EVP_MD_CTX_new();
        if (!mdCtx)
            failOpenSSLError("EVP_MD_CTX_new");

        const EVP_MD *md = digestCache.checkCache(_algorithm_name);

        if (EVP_DigestVerifyInit(mdCtx, nullptr, md, nullptr, publicKey.get()) <= 0)
            failOpenSSLError("EVP_DigestVerifyInit (pkVerifySignature)");

        if (EVP_DigestVerifyUpdate(mdCtx, _signedData, len_signedData) <= 0)
            failOpenSSLError("EVP_DigestVerifyUpdate (pkVerifySignature)");

        // Perform the actual verification
        size32_t res = EVP_DigestVerifyFinal(mdCtx, reinterpret_cast<const unsigned char *>(_signature), len_signature);

        // Clean up
        EVP_MD_CTX_free(mdCtx);
        return res == 1;
    }
    catch (...)
    {
        if (mdCtx)
            EVP_MD_CTX_free(mdCtx);
        throw;
    }
}
