/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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
#include "jexcept.hpp"
#include "digisign.hpp"
#include "ske.hpp"
#include <openssl/pem.h>
#include <openssl/err.h>
#include <openssl/rand.h>
#include <string>
#include <unordered_map>
#include "jthread.hpp"

#include "cryptolib.hpp"

using namespace std;

#define CRYPTOLIB_VERSION "CRYPTOLIB 1.0.00"

using namespace cryptohelper;


static const char * EclDefinition = nullptr;//Definitions specified in lib_cryptolib.ecllib

CRYPTOLIB_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb)
{
    //  Warning:    This function may be called without the plugin being loaded fully.  
    //              It should not make any library calls or assume that dependent modules
    //              have been loaded or that it has been initialized.
    //
    //              Specifically:  "The system does not call DllMain for process and thread 
    //              initialization and termination.  Also, the system does not load 
    //              additional executable modules that are referenced by the specified module."

    if (pb->size != sizeof(ECLPluginDefinitionBlock))
        return false;

    pb->magicVersion = PLUGIN_VERSION;
    pb->version = CRYPTOLIB_VERSION;
    pb->moduleName = "lib_cryptolib";
    pb->ECL = EclDefinition;
    pb->flags = PLUGIN_IMPLICIT_MODULE;
    pb->description = "CryptoLib cryptography services library";
    return true;
}

namespace nsCryptolib {
    IPluginContext * parentCtx = nullptr;
}
using namespace nsCryptolib;

CRYPTOLIB_API void setPluginContext(IPluginContext * _ctx) { parentCtx = _ctx; }

//-------------------------------------------------------------------------------------------------------------------------------------------
//  C++ to ECL helpers
//-------------------------------------------------------------------------------------------------------------------------------------------

size32_t addToECLSetOfString(void * set, const char * str)
{
    size32_t len = strlen(str);
    if (len)
    {
        memcpy(set, &len, sizeof(len));//copy 4 byte length. String Set memory buffer should already be allocated within the context
        memcpy((char *)set + sizeof(len), str, strlen(str));//followed by string
    }
    return len + sizeof(len);//return length copied to memory buffer
}

void stringArrayToECLSetOfString(const StringArray & arr, void * * set, size32_t * len)
{
    //STRING is a variable length string stored as a 4 byte length, followed by the string.  A set of strings has
    //the strings in this format repeated, and the total size returned in the __lenResult parameter.  The __result value should be allocated
    //on the heap.
    size32_t currLen = 0;
    for (aindex_t idx = 0; idx < arr.length(); idx++)
        currLen += strlen(arr.item(idx)) + sizeof(size32_t);
    void * pSet = CTXMALLOC(parentCtx, currLen);
    assertex(set);

    *len = currLen;
    *set = pSet;
    currLen = 0;
    for (aindex_t idx = 0; idx < arr.length(); idx++)
        currLen += addToECLSetOfString((char*)pSet + currLen, arr.item(idx));
}

//-------------------------------------------------------------------------------------------------------------------------------------------
// CIPHER SYMMETRIC SUPPORT
//-------------------------------------------------------------------------------------------------------------------------------------------

//NB: It should be noted that we count on the cryptohelper library to call OpenSSL_add_all_algorithms() at init time
void my_clAlgorithmsCallback(const OBJ_NAME * _obj, void * _pRes)
{
    ((StringArray*)_pRes)->append((const char*)_obj->name);//add this digest to string array
}

//NB: CRYPTOLIB call signatures can be gleaned from the generated workunit CPP file

//Symmetric block/stream digest algorithms
CRYPTOLIB_API void CRYPTOLIB_CALL clInstalledSymmetricCipherAlgorithms(bool & __isAllResult, size32_t & __lenResult, void * & __result)
{
    __isAllResult = false;
    StringArray algorithms;
    OBJ_NAME_do_all_sorted(OBJ_NAME_TYPE_CIPHER_METH, my_clAlgorithmsCallback, (void*)&algorithms);
    stringArrayToECLSetOfString(algorithms, &__result, &__lenResult);
}

CRYPTOLIB_API void CRYPTOLIB_CALL clSupportedSymmetricCipherAlgorithms(bool & __isAllResult, size32_t & __lenResult, void * & __result)
{
    __isAllResult = false;
    StringArray algorithms;
    algorithms.appendList("aes-256-cbc,aes-192-cbc,aes-128-cbc", ",");//make sure this list matches loadEVP_Cipher()
    stringArrayToECLSetOfString(algorithms, &__result, &__lenResult);
}


//-------------------------------------------------------------------------------------------------------------------------------------------

//Helpers to build the DATA buffer returned from clSymmetricEncrypt, and sent to clSymmetricDecrypt
//Buffer is structured as:
// (size32_t)lenIV, IV excluding NULL, (size32_t)LenPlainText excluding NULL, (size32_t)LenCipher, Cipher
static void symmDeserialize(const void * pBuffer, StringBuffer & sbIV, StringBuffer & sbCipher, size32_t * lenPlainText)
{
    size32_t len;
    const char * finger = (const char *)pBuffer;

    memcpy(&len, finger, sizeof(size32_t));//extract IV
    finger += sizeof(size32_t);
    sbIV.append(len, finger);

    finger += len;
    memcpy(lenPlainText, finger, sizeof(size32_t));//extract length of plain text

    finger += sizeof(size32_t);
    memcpy(&len, finger, sizeof(size32_t));//extract cipher
    finger += sizeof(size32_t);
    sbCipher.append(len, finger);
}

static void symmSerialize(void * & result, size32_t & lenResult, const char * pIV, size32_t lenIV, size32_t lenPlainText, size32_t lenCipherBuff, const void * pCipherBuff)
{
    //Allocate return DATA buffer
    lenResult = sizeof(size32_t) + lenIV + sizeof(size32_t) + sizeof(size32_t) + lenCipherBuff;
    result = CTXMALLOC(parentCtx, lenResult);

    //build result DATA buffer in the form:

    char * pRes = (char *)result;
    memcpy(pRes, &lenIV, sizeof(size32_t));//copy size of IV
    pRes += sizeof(size32_t);
    memcpy(pRes, pIV, lenIV);//copy the IV
    pRes += lenIV;
    memcpy(pRes, &lenPlainText, sizeof(size32_t));
    pRes += sizeof(size32_t);
    memcpy(pRes, &lenCipherBuff, sizeof(size32_t));
    pRes += sizeof(size32_t);
    memcpy(pRes, pCipherBuff, lenCipherBuff);
}

//-------------------------------------------------------------------------------------------------------------------------------------------

void verifySymmetricAlgorithm(const char * algorithm, size32_t len)
{
     if (strieq(algorithm, "aes-128-cbc"))
     {
         if (len != 16)
             throw MakeStringException(-1, "Invalid Key Length %d specified for algorithm %s, try 16", len, algorithm);
     }
     else if (strieq(algorithm, "aes-192-cbc"))
     {
         if (len != 24)
             throw MakeStringException(-1, "Invalid Key Length %d specified for algorithm %s, try 24", len, algorithm);
     }
     else if (strieq(algorithm, "aes-256-cbc"))
     {
         if (len != 32)
             throw MakeStringException(-1, "Invalid Key Length %d specified for algorithm %s, try 32", len, algorithm);
     }
     else
         throw MakeStringException(-1, "Unsupported symmetric algorithm (%s) specified", algorithm);
}

CRYPTOLIB_API void CRYPTOLIB_CALL clSymmetricEncrypt(size32_t & __lenResult, void * & __result,
                                                    const char * algorithm,
                                                    const char * key,
                                                    size32_t lenInputdata,const void * inputdata)
{
    verifySymmetricAlgorithm(algorithm, strlen(key) );

    //Create a unique Initialization Vector
    unsigned char iv[EVP_MAX_IV_LENGTH];
    RAND_bytes(iv, EVP_MAX_IV_LENGTH);

    //temporary buffer for result
    MemoryBuffer out;
    aesEncrypt(out, lenInputdata, inputdata, strlen(key), key, (const char *)iv);

    //build result DATA buffer in the form:
    // (size32_t)EVP_MAX_IV_LENGTH, IV, (size32_t)LenPlainText excluding NULL, (size32_t)LenCipher, Cipher
    symmSerialize(__result, __lenResult, (const char *)iv, sizeof(iv), lenInputdata, out.length(), (const void *)out.bufferBase());
}

CRYPTOLIB_API void CRYPTOLIB_CALL clSymmetricDecrypt(size32_t & __lenResult,void * & __result,
                                                const char * algorithm,
                                                const char * key,
                                                size32_t lenEncrypteddata,const void * encrypteddata)
{
    verifySymmetricAlgorithm(algorithm, strlen(key));

    //Decompose DATA buffer

    StringBuffer sbIV;
    StringBuffer sbCipher;
    size32_t     lenPlainText;
    symmDeserialize(encrypteddata, sbIV, sbCipher, &lenPlainText);

    __result = (char *)CTXMALLOC(parentCtx, lenPlainText);
    __lenResult = lenPlainText;

    MemoryBuffer decrypted;
    size32_t len = aesDecrypt(decrypted, sbCipher.length(), sbCipher.str(), strlen(key), key, sbIV.str());
    memcpy(__result, decrypted.toByteArray(), __lenResult);
}



//-------------------------------------------------------------------------------------------------------------------------------------------
// HASH SUPPORT
//-------------------------------------------------------------------------------------------------------------------------------------------

CRYPTOLIB_API void CRYPTOLIB_CALL clInstalledHashAlgorithms(bool & __isAllResult, size32_t & __lenResult, void * & __result)
{
    __isAllResult = false;
    StringArray algorithms;
    OBJ_NAME_do_all_sorted(OBJ_NAME_TYPE_MD_METH, my_clAlgorithmsCallback, (void*)&algorithms);
    stringArrayToECLSetOfString(algorithms, &__result, &__lenResult);
}

CRYPTOLIB_API void CRYPTOLIB_CALL clSupportedHashAlgorithms(bool & __isAllResult, size32_t & __lenResult, void * & __result)
{
    __isAllResult = false;
    StringArray algorithms;
    algorithms.appendList("SHA1,SHA224,SHA256,SHA384,SHA512", ",");//make sure these match algorithms in clHash()
    stringArrayToECLSetOfString(algorithms, &__result, &__lenResult);
}

void throwHashError(const char * str)
{
    unsigned long err = ERR_get_error();
    char errStr[1024];
    ERR_error_string_n(err, errStr, sizeof(errStr));
    throw MakeStringException(-1, "%s : ERROR %ld (0x%lX) : %s", str, err, err, errStr);
}

CRYPTOLIB_API void CRYPTOLIB_CALL clHash(size32_t & __lenResult, void * & __result,
                                         const char * algorithm,
                                         size32_t lenInputData, const void * inputData)
{
    //Make sure each algorithm is included in clHashAlgorithms()
    if (strieq("SHA1", algorithm))
    {
        SHA_CTX context;
        if (!SHA1_Init(&context))
            throwHashError("OpenSSL ERROR calling SHA1_Init");

        if (!SHA1_Update(&context, (unsigned char*)inputData, lenInputData))
            throwHashError("OpenSSL ERROR calling SHA1_Update");

        __lenResult = SHA_DIGEST_LENGTH;//20 bytes
        __result = CTXMALLOC(parentCtx, __lenResult);

        if (!SHA1_Final((unsigned char *)__result, &context))
        {
            free(__result);
            throwHashError("OpenSSL ERROR calling SHA1_Final");
        }
    }
    else if (strieq("SHA224", algorithm))
    {
        SHA256_CTX context;//SHA224 uses the SHA256 context
        if (!SHA224_Init(&context))
            throwHashError("OpenSSL ERROR calling SHA224_Init");

        if (!SHA224_Update(&context, (unsigned char*)inputData, lenInputData))
            throwHashError("OpenSSL ERROR calling SHA224_Update");

        __lenResult = SHA224_DIGEST_LENGTH;//28 bytes
        __result = CTXMALLOC(parentCtx, __lenResult);

        if (!SHA224_Final((unsigned char *)__result, &context))
        {
            free(__result);
            throwHashError("OpenSSL ERROR calling SHA224_Final");
        }
    }
    else if (strieq("SHA256", algorithm))
    {
        SHA256_CTX context;
        if (!SHA256_Init(&context))
            throwHashError("OpenSSL ERROR calling SHA256_Init");

        if (!SHA256_Update(&context, (unsigned char*)inputData, lenInputData))
            throwHashError("OpenSSL ERROR calling SHA256_Update");

        __lenResult = SHA256_DIGEST_LENGTH;//32 bytes
        __result = CTXMALLOC(parentCtx, __lenResult);

        if (!SHA256_Final((unsigned char *)__result, &context))
        {
            free(__result);
            throwHashError("OpenSSL ERROR calling SHA256_Final");
        }
    }
    else if (strieq("SHA384", algorithm))
    {
        SHA512_CTX context;//SHA384 uses the SHA512 context
        if (!SHA384_Init(&context))
            throwHashError("OpenSSL ERROR calling SHA384_Init");

        if (!SHA384_Update(&context, (unsigned char*)inputData, lenInputData))
            throwHashError("OpenSSL ERROR calling SHA384_Update");

        __lenResult = SHA384_DIGEST_LENGTH;//48 bytes
        __result = CTXMALLOC(parentCtx, __lenResult);

        if (!SHA384_Final((unsigned char *)__result, &context))
        {
            free(__result);
            throwHashError("OpenSSL ERROR calling SHA384_Final");
        }
    }
    else if (strieq("SHA512", algorithm))
    {
        SHA512_CTX context;
        if (!SHA512_Init(&context))
            throwHashError("OpenSSL ERROR calling SHA512_Init");

        if (!SHA512_Update(&context, (unsigned char*)inputData, lenInputData))
            throwHashError("OpenSSL ERROR calling SHA512_Update");

        __lenResult = SHA512_DIGEST_LENGTH;//64 bytes
        __result = CTXMALLOC(parentCtx, __lenResult);

        if (!SHA512_Final((unsigned char *)__result, &context))
        {
            free(__result);
            throwHashError("OpenSSL ERROR calling SHA512_Final");
        }
    }
    else
    {
        throw MakeStringException(-1, "Unsupported hash algorithm '%s' specified", algorithm);
    }
}

//-------------------------------------------------------------------------------------------------------------------------------------------
// PUBLIC KEY SUPPORT
//-------------------------------------------------------------------------------------------------------------------------------------------

CRYPTOLIB_API void CRYPTOLIB_CALL clInstalledPublicKeyAlgorithms(bool & __isAllResult, size32_t & __lenResult, void * & __result)
{
    __isAllResult = false;
    StringArray algorithms;
    OBJ_NAME_do_all_sorted(OBJ_NAME_TYPE_PKEY_METH, my_clAlgorithmsCallback, (void*)&algorithms);
    stringArrayToECLSetOfString(algorithms, &__result, &__lenResult);
}

CRYPTOLIB_API void CRYPTOLIB_CALL clSupportedPublicKeyAlgorithms(bool & __isAllResult, size32_t & __lenResult, void * & __result)
{
    __isAllResult = false;
    StringArray algorithms;
    algorithms.appendList("RSA", ",");
    stringArrayToECLSetOfString(algorithms, &__result, &__lenResult);
}

//signature helper function
void doPKISign(size32_t & __lenResult, void * & __result,
        IDigitalSignatureManager * pDSM,
        size32_t lenInputdata, const void * inputdata)
{
    StringBuffer sbSig;
    if (pDSM->digiSign(sbSig, lenInputdata, inputdata))
    {
        __result = CTXMALLOC(parentCtx, sbSig.length());
        __lenResult = sbSig.length();
        memcpy(__result, sbSig.str(), __lenResult);
    }
    else
    {
        throw MakeStringException(-1, "Unable to create Digital Signature");
    }
}

void verifyPKIAlgorithm(const char * pkAlgorithm)
{
    if (!strieq(pkAlgorithm, "RSA"))
        throw MakeStringException(-1, "Unsupported PKI algorithm (%s) specified", pkAlgorithm);
}


//-----------------------------------------------------------------
//  Simple cache for instances of Digital Signature Managers
//-----------------------------------------------------------------
class CDSMCache
{
private:
    CriticalSection csDSMCache;//guards modifications to the cache map
    typedef std::unordered_map<string, Owned<IDigitalSignatureManager>> DSMCache;
    DSMCache dsmCache;

public:
    IDigitalSignatureManager * getInstance(const char * algo, const char * pubKeyFS, const char * pubKeyBuff, const char * privKeyFS, const char * privKeyBuff, const char * passphrase)
    {
        VStringBuffer searchKey("%s_%s_%s_%s_%s_%s", algo, isEmptyString(pubKeyFS) ? "" : pubKeyFS, isEmptyString(pubKeyBuff) ? "" : pubKeyBuff,
                                isEmptyString(privKeyFS) ? "" : privKeyFS, isEmptyString(privKeyBuff) ? "" : privKeyBuff, passphrase);
        CriticalBlock block(csDSMCache);
        DSMCache::iterator it = dsmCache.find(searchKey.str());
        IDigitalSignatureManager * ret = nullptr;
        if (it != dsmCache.end())//exists in cache
        {
            ret = (*it).second;
        }
        else
        {
            if (!isEmptyString(pubKeyFS) || !isEmptyString(privKeyFS))
                ret = createDigitalSignatureManagerInstanceFromFiles(pubKeyFS, privKeyFS, passphrase);
            else
                ret = createDigitalSignatureManagerInstanceFromKeys(pubKeyBuff, privKeyBuff, passphrase);

            dsmCache.emplace(searchKey.str(), ret);
        }
        return LINK(ret);
    }
};
static CDSMCache g_DSMCache;

//Sign given data using private key
CRYPTOLIB_API void CRYPTOLIB_CALL clPKISign(size32_t & __lenResult,void * & __result,
                                        const char * pkalgorithm,
                                        const char * privatekeyfile,
                                        const char * passphrase,
                                        size32_t lenInputdata,const void * inputdata)
{
    verifyPKIAlgorithm(pkalgorithm);//TODO extend cryptohelper to support more algorithms
    Owned<IDigitalSignatureManager> pDSM = g_DSMCache.getInstance(pkalgorithm, nullptr, nullptr, privatekeyfile, nullptr, passphrase);
    if (pDSM)
    {
        doPKISign(__lenResult, __result, pDSM, lenInputdata, inputdata);
    }
    else
    {
        throw MakeStringException(-1, "Unable to create Digital Signature Manager");
    }
}

CRYPTOLIB_API void CRYPTOLIB_CALL clPKISignBuff(size32_t & __lenResult, void * & __result,
                                            const char * pkalgorithm,
                                            const char * privatekeybuff,
                                            const char * passphrase,
                                            size32_t lenInputdata, const void * inputdata)
{
    verifyPKIAlgorithm(pkalgorithm);
    Owned<IDigitalSignatureManager> pDSM = g_DSMCache.getInstance(pkalgorithm, nullptr, nullptr, nullptr, privatekeybuff, passphrase);
    if (pDSM)
    {
        doPKISign(__lenResult, __result, pDSM, lenInputdata, inputdata);
    }
    else
    {
        throw MakeStringException(-1, "Unable to create Digital Signature Manager");
    }
}

CRYPTOLIB_API bool CRYPTOLIB_CALL clPKIVerifySignature(const char * pkalgorithm,
                                                    const char * publickeyfile,
                                                    const char * passphrase,
                                                    size32_t lenSignature,const void * signature,
                                                    size32_t lenSigneddata,const void * signeddata)
{
    verifyPKIAlgorithm(pkalgorithm);
    Owned<IDigitalSignatureManager> pDSM = g_DSMCache.getInstance(pkalgorithm, publickeyfile, nullptr, nullptr, nullptr, passphrase);
    if (pDSM)
    {
        StringBuffer sbSig(lenSignature, (const char *)signature);
        bool rc = pDSM->digiVerify(sbSig.str(), lenSigneddata, signeddata);
        return rc;
    }
    else
    {
        throw MakeStringException(-1, "Unable to create Digital Signature Manager");
    }
}

CRYPTOLIB_API bool CRYPTOLIB_CALL clPKIVerifySignatureBuff(const char * pkalgorithm,
                                                       const char * publicKeyBuff,
                                                       const char * passphrase,
                                                       size32_t lenSignature, const void * signature,
                                                       size32_t lenSigneddata, const void * signeddata)
{
    verifyPKIAlgorithm(pkalgorithm);
    Owned<IDigitalSignatureManager> pDSM = g_DSMCache.getInstance(pkalgorithm, nullptr, publicKeyBuff, nullptr, nullptr, passphrase);
    if (pDSM)
    {
        StringBuffer sbSig(lenSignature, (const char *)signature);
        bool rc = pDSM->digiVerify(sbSig.str(), lenSigneddata, signeddata);
        return rc;
    }
    else
    {
        throw MakeStringException(-1, "Unable to create Digital Signature Manager");
    }
}


//-----------------------------------------------------------------
//  Simple cache for loaded keys
//
//-----------------------------------------------------------------
#ifdef _USE_HASHMAP
class CKeyCache : public CInterface
{
private:
    typedef std::unordered_map<string, Owned<CLoadedKey>> KeyCache;
    KeyCache keyCache;

public:

    CLoadedKey * getInstance(bool isPublic, const char * keyFS, const char * keyBuff, const char * passphrase)
    {
        if (isEmptyString(keyFS) && isEmptyString(keyBuff))
            throw MakeStringException(-1, "Must specify a key filename or provide a key buffer");
        VStringBuffer searchKey("%s_%s_%s", isEmptyString(keyFS) ? "" : keyFS, isEmptyString(keyBuff) ? "" : keyBuff, isEmptyString(passphrase) ? "" : passphrase);
        KeyCache::iterator it = keyCache.find(searchKey.str());
        CLoadedKey * ret = nullptr;
        if (it != keyCache.end())//exists in cache?
        {
            ret = (*it).second;
        }
        else
        {
            if (!isEmptyString(keyFS))
            {
                //Create CLoadedKey from filespec
                if (isPublic)
                    ret = loadPublicKeyFromFile(keyFS, passphrase);
                else
                    ret = loadPrivateKeyFromFile(keyFS, passphrase);
            }
            else
            {
                //Create CLoadedKey from key contents
                if (isPublic)
                    ret = loadPublicKeyFromMemory(keyBuff, passphrase);
                else
                    ret = loadPrivateKeyFromMemory(keyBuff, passphrase);
            }

            keyCache.emplace(searchKey.str(), ret);
        }
        return LINK(ret);
    }
};

#else

class CKeyCache : public CInterface
{
private:
    bool        m_isPublic = false;
    StringAttr  m_keyFS;
    StringAttr  m_keyBuff;
    StringAttr  m_passphrase;
    Owned<CLoadedKey> m_loadedKey;

    //String compare that treats null ptr and ptr to empty string as matching
    inline bool sameString(const char * left, const char * right)
    {
        if (isEmptyString(left))
            return isEmptyString(right);
        else if (isEmptyString(right))
            return false;
        return strcmp(left, right) == 0;
    }

public:

    CLoadedKey * getInstance(bool isPublic, const char * keyFS, const char * keyBuff, const char * passphrase)
    {
        if (!m_loadedKey ||
            isPublic != m_isPublic ||
            !sameString(passphrase, m_passphrase.str())  ||
            !sameString(keyFS, m_keyFS.str()) ||
            !sameString(keyBuff, m_keyBuff.str()))
        {
            CLoadedKey *newKey;

            if (!isEmptyString(keyFS))
            {
                //Create CLoadedKey from filespec
                if (isPublic)
                    newKey = loadPublicKeyFromFile(keyFS, passphrase);
                else
                    newKey = loadPrivateKeyFromFile(keyFS, passphrase);
            }
            else if (!isEmptyString(keyBuff))
            {
                //Create CLoadedKey from key contents
                if (isPublic)
                    newKey = loadPublicKeyFromMemory(keyBuff, passphrase);
                else
                    newKey = loadPrivateKeyFromMemory(keyBuff, passphrase);
            }
            else
                throw makeStringException(-1, "Must specify a key filename or provide a key buffer");

            m_loadedKey.setown(newKey);//releases previous ptr
            m_isPublic = isPublic;
            m_keyFS.set(keyFS);
            m_keyBuff.set(keyBuff);
            m_passphrase.set(passphrase);
        }
        return LINK(m_loadedKey);
    }
};
#endif//_USE_HASHMAP

//----------------------------------------------------------------------------
// TLS storage of Key cache
//----------------------------------------------------------------------------
static thread_local CKeyCache *pKC = nullptr;

static bool clearupKeyCache(bool isPooled)
{
    delete pKC;
    pKC = nullptr;
    return false;
}

static CLoadedKey * getCachedKey(bool isPublic, const char * keyFS, const char * keyBuff, const char * passphrase)
{
    if (!pKC)
    {
        pKC = new CKeyCache();
        addThreadTermFunc(clearupKeyCache);
    }
    return pKC->getInstance(isPublic, keyFS, keyBuff, passphrase);
}
//------------------------------------
//Encryption helper
//------------------------------------
static void doPKIEncrypt(size32_t & __lenResult,void * & __result,
                  CLoadedKey * publicKey,
                  size32_t lenInputdata,const void * inputdata)
{
    MemoryBuffer pkeMb;
    __lenResult = publicKeyEncrypt(pkeMb, lenInputdata, inputdata, *publicKey);
    if (__lenResult)
    {
        __result = CTXMALLOC(parentCtx, __lenResult);
        memcpy(__result, pkeMb.bytes(), __lenResult);
    }
}

//------------------------------------
//Decryption helper
//------------------------------------
static void doPKIDecrypt(size32_t & __lenResult,void * & __result,
                  CLoadedKey * privateKey,
                  size32_t lenInputdata,const void * inputdata)
{
    MemoryBuffer pkeMb;
    __lenResult = privateKeyDecrypt(pkeMb, lenInputdata, inputdata, *privateKey);
    if (__lenResult)
    {
        __result = CTXMALLOC(parentCtx, __lenResult);
        memcpy(__result, pkeMb.bytes(), __lenResult);
    }
}



//encryption functions that take filespecs of key files
CRYPTOLIB_API void CRYPTOLIB_CALL clPKIEncrypt(size32_t & __lenResult,void * & __result,
                                            const char * pkalgorithm,
                                            const char * publickeyfile,
                                            const char * passphrase,
                                            size32_t lenInputdata,const void * inputdata)
{
    verifyPKIAlgorithm(pkalgorithm);
    Owned<CLoadedKey> publicKey = getCachedKey(true, publickeyfile, nullptr, passphrase);
    doPKIEncrypt(__lenResult, __result, publicKey, lenInputdata, inputdata);
}


CRYPTOLIB_API void CRYPTOLIB_CALL clPKIDecrypt(size32_t & __lenResult,void * & __result,
                                            const char * pkalgorithm,
                                            const char * privatekeyfile,
                                            const char * passphrase,
                                            size32_t lenEncrypteddata,const void * encrypteddata)
{
    verifyPKIAlgorithm(pkalgorithm);
    Owned<CLoadedKey> privateKey = getCachedKey(false, privatekeyfile, nullptr, passphrase);
    doPKIDecrypt(__lenResult, __result, privateKey, lenEncrypteddata, encrypteddata);
}

//encryption functions that take keys in a buffer

CRYPTOLIB_API void CRYPTOLIB_CALL clPKIEncryptBuff(size32_t & __lenResult,void * & __result,
                                                const char * pkalgorithm,
                                                const char * publickeybuff,
                                                const char * passphrase,
                                                size32_t lenInputdata,const void * inputdata)
{
    verifyPKIAlgorithm(pkalgorithm);
    Owned<CLoadedKey> publicKey = getCachedKey(true, nullptr, publickeybuff, passphrase);
    doPKIEncrypt(__lenResult, __result, publicKey, lenInputdata, inputdata);
}

CRYPTOLIB_API void CRYPTOLIB_CALL clPKIDecryptBuff(size32_t & __lenResult,void * & __result,
                                                const char * pkalgorithm,
                                                const char * privatekeybuff,
                                                const char * passphrase,
                                                size32_t lenEncrypteddata,const void * encrypteddata)
{
    verifyPKIAlgorithm(pkalgorithm);
    Owned<CLoadedKey> privateKey = getCachedKey(false, nullptr, privatekeybuff, passphrase);
    doPKIDecrypt(__lenResult, __result, privateKey, lenEncrypteddata, encrypteddata);
}
