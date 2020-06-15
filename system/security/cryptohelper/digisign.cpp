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
#include "jliball.hpp"
#if defined(_USE_OPENSSL)
#include <openssl/pem.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#endif
#include "jencrypt.hpp"
#include "digisign.hpp"
#include <mutex>


namespace cryptohelper
{

#if defined(_USE_OPENSSL)


//Create base 64 encoded digital signature of given data
bool digiSign(StringBuffer &b64Signature, size32_t dataSz, const void *data, const CLoadedKey &signingKey)
{
    OwnedEVPMdCtx signingCtx(EVP_MD_CTX_create());
    //initialize context for SHA-256 hashing function
    int rc = EVP_DigestSignInit(signingCtx, nullptr, EVP_sha256(), nullptr, signingKey);
    if (rc <= 0)
        throwEVPException(-1, "digiSign:EVP_DigestSignInit");

    //add string to the context
    if (EVP_DigestSignUpdate(signingCtx, data, dataSz) <= 0)
        throwEVPException(-1, "digiSign:EVP_DigestSignUpdate");

    //compute length of signature
    size_t encMsgLen;
    if (EVP_DigestSignFinal(signingCtx, nullptr, &encMsgLen) <= 0)
        throwEVPException(-1, "digiSign:EVP_DigestSignFinal1");

    if (encMsgLen == 0)
        throwEVPException(-1, "digiSign:EVP_DigestSignFinal length returned 0");

    //compute signature (signed digest)
    OwnedEVPMemory encMsg = OPENSSL_malloc(encMsgLen);
    if (encMsg == nullptr)
        throw MakeStringException(-1, "digiSign:OPENSSL_malloc(%u) returned NULL", (unsigned)encMsgLen);

    if (EVP_DigestSignFinal(signingCtx, (unsigned char *)encMsg.get(), &encMsgLen) <= 0)
        throwEVPException(-1, "digiSign:EVP_DigestSignFinal2");

    //convert to base64
    JBASE64_Encode(encMsg, encMsgLen, b64Signature, false);

    return true;
}

//Verify the given data was used to create the given digital signature
bool digiVerify(const char *b64Signature, size32_t dataSz, const void *data, const CLoadedKey &verifyingKey)
{
    OwnedEVPMdCtx verifyingCtx(EVP_MD_CTX_create());
    int rc = EVP_DigestVerifyInit(verifyingCtx, nullptr, EVP_sha256(), nullptr, verifyingKey);
    if (rc <= 0)
        throwEVPException(-1, "digiVerify:EVP_DigestVerifyInit");

    //decode base64 signature
    StringBuffer decodedSig;
    JBASE64_Decode(b64Signature, decodedSig);

    if (EVP_DigestVerifyUpdate(verifyingCtx, data, dataSz) <= 0)
        throwEVPException(-1, "digiVerify:EVP_DigestVerifyUpdate");

    return 1 == EVP_DigestVerifyFinal(verifyingCtx, (unsigned char *)decodedSig.str(), decodedSig.length());
}


class CDigitalSignatureManager : public CSimpleInterfaceOf<IDigitalSignatureManager>
{
private:
    Linked<CLoadedKey> pubKey, privKey;
    bool         signingConfigured = false;
    bool         verifyingConfigured = false;

public:
    CDigitalSignatureManager(CLoadedKey *_pubKey, CLoadedKey *_privKey) : pubKey(_pubKey), privKey(_privKey)
    {
        signingConfigured = nullptr != privKey.get();
        verifyingConfigured = nullptr != pubKey.get();
    }

    virtual bool isDigiSignerConfigured() const override
    {
        return signingConfigured;
    }

    virtual bool isDigiVerifierConfigured() const override
    {
        return verifyingConfigured;
    }

    //Create base 64 encoded digital signature of given data
    virtual bool digiSign(StringBuffer & b64Signature, size32_t dataSz, const void *data) const override
    {
        if (!signingConfigured)
            throw MakeStringException(-1, "digiSign:Creating Digital Signatures not configured");

        return cryptohelper::digiSign(b64Signature, dataSz, data, *privKey);
    }

    virtual bool digiSign(StringBuffer & b64Signature, const char *text) const override
    {
        return digiSign(b64Signature, strlen(text), text);
    }

    //Verify the given data was used to create the given digital signature
    virtual bool digiVerify(const char *b64Signature, size32_t dataSz, const void *data) const override
    {
        if (!verifyingConfigured)
            throw MakeStringException(-1, "digiVerify:Verifying Digital Signatures not configured");

        return cryptohelper::digiVerify(b64Signature, dataSz, data, *pubKey);
    }

    virtual bool digiVerify(const char *b64Signature, const char *text) const override
    {
        return digiVerify(b64Signature, strlen(text), text);
    }

    virtual const char * queryKeyName() const override
    {
        return pubKey->queryKeyName();
    }
};

#else

//Dummy implementation if no OPENSSL available.

bool digiSign(StringBuffer &b64Signature, const char *text, const CLoadedKey &signingKey)
{
    throwStringExceptionV(-1, "digiSign: unavailable without openssl");
}

bool digiVerify(const char *b64Signature, const char *text, const CLoadedKey &verifyingKey)
{
    throwStringExceptionV(-1, "digiVerify: unavailable without openssl");
}

class CDigitalSignatureManager : public CSimpleInterfaceOf<IDigitalSignatureManager>
{
public:
    CDigitalSignatureManager(const char * _pubKeyBuff, const char * _privKeyBuff, const char * _passPhrase)
    {
        WARNLOG("CDigitalSignatureManager: Platform built without OPENSSL!");
    }

    virtual bool isDigiSignerConfigured() const override
    {
        return false;
    }

    virtual bool isDigiVerifierConfigured() const override
    {
        return false;
    }


    virtual bool digiSign(StringBuffer & b64Signature, size32_t dataSz, const void *data) const override
    {
        throwStringExceptionV(-1, "digiSign: unavailable without openssl");
    }

    virtual bool digiSign(StringBuffer & b64Signature, const char * text) const override
    {
        throwStringExceptionV(-1, "digiSign: unavailable without openssl");
    }

    virtual bool digiVerify(const char *b64Signature, const char * text) const override
    {
        throwStringExceptionV(-1, "digiVerify: unavailable without openssl");
    }

    virtual bool digiVerify(const char *b64Signature, size32_t dataSz, const void *data) const override
    {
        throwStringExceptionV(-1, "digiVerify: unavailable without openssl");
    }

    virtual const char * queryKeyName() const override
    {
        throwStringExceptionV(-1, "digiVerify: unavailable without openssl");
    }
};

#endif


static IDigitalSignatureManager * dsm = nullptr;
static std::once_flag dsmInitFlag;

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}
MODULE_EXIT()
{
    ::Release(dsm);
}

#if defined(_USE_OPENSSL) && !defined(_WIN32)
static void createDigitalSignatureManagerInstance(IDigitalSignatureManager * * ppDSM)
{
    const char * pubKey = nullptr, *privKey = nullptr, *passPhrase = nullptr;
    queryHPCCPKIKeyFiles(nullptr, &pubKey, &privKey, &passPhrase);
    StringBuffer passPhraseDec;
    if (!isEmptyString(passPhrase))
    {
        decrypt(passPhraseDec, passPhrase);
        passPhrase = passPhraseDec.str();
    }
    *ppDSM = createDigitalSignatureManagerInstanceFromFiles(pubKey, privKey, passPhrase);
}
#endif


//Returns reference to singleton instance created from environment.conf key file settings
IDigitalSignatureManager * queryDigitalSignatureManagerInstanceFromEnv()
{
#if defined(_USE_OPENSSL) && !defined(_WIN32)
    std::call_once(dsmInitFlag, createDigitalSignatureManagerInstance, &dsm);
    return dsm;
#else
    return nullptr;
#endif
}

//Create using given key filespecs
//Caller must release when no longer needed
IDigitalSignatureManager * createDigitalSignatureManagerInstanceFromFiles(const char * pubKeyFileName, const char *privKeyFileName, const char * passPhrase)
{
    return createDigitalSignatureManagerInstanceFromFiles(pubKeyFileName, privKeyFileName, passPhrase ? strlen(passPhrase) : 0, (const void *)passPhrase);
}

IDigitalSignatureManager * createDigitalSignatureManagerInstanceFromFiles(const char * pubKeyFileName, const char *privKeyFileName, size32_t lenPassphrase, const void * passPhrase)
{
#if defined(_USE_OPENSSL) && !defined(_WIN32)
    Owned<CLoadedKey> pubKey, privKey;
    Owned<IMultiException> exceptions;
    if (!isEmptyString(pubKeyFileName))
    {
        try
        {
            pubKey.setown(loadPublicKeyFromFile(pubKeyFileName));
        }
        catch (IException * e)
        {
            if (!exceptions)
                exceptions.setown(makeMultiException("createDigitalSignatureManagerInstanceFromFiles"));

            exceptions->append(* makeWrappedExceptionV(e, -1, "createDigitalSignatureManagerInstanceFromFiles:Cannot load public key file"));
            e->Release();
        }
    }

    if (!isEmptyString(privKeyFileName))
    {
        try
        {
            privKey.setown(loadPrivateKeyFromFile(privKeyFileName, lenPassphrase, passPhrase));
        }
        catch (IException * e)
        {
            if (!exceptions)
                exceptions.setown(makeMultiException("createDigitalSignatureManagerInstanceFromFiles"));

            exceptions->append(* makeWrappedExceptionV(e, -1, "createDigitalSignatureManagerInstanceFromFiles:Cannot load private key file"));
            e->Release();
        }
    }

    // NB: allow it continue if 1 of the keys successfully loaded.
    if (exceptions && exceptions->ordinality())
    {
        if (!pubKey && !privKey)
            throw exceptions.getClear();
        else
            EXCLOG(exceptions, nullptr);
    }

    return new CDigitalSignatureManager(pubKey, privKey);
#else
    return nullptr;
#endif
}

//Create using given PEM formatted keys
//Caller must release when no longer needed
IDigitalSignatureManager * createDigitalSignatureManagerInstanceFromKeys(const char * pubKeyString, const char * privKeyString, const char * passPhrase)
{
    return createDigitalSignatureManagerInstanceFromKeys(pubKeyString, privKeyString, passPhrase ? strlen(passPhrase) : 0, (const void *)passPhrase);
}

IDigitalSignatureManager * createDigitalSignatureManagerInstanceFromKeys(const char * pubKeyString, const char * privKeyString, size32_t lenPassphrase, const void * passPhrase)
{
#if defined(_USE_OPENSSL) && !defined(_WIN32)
    Owned<CLoadedKey> pubKey, privKey;

    Owned<IMultiException> exceptions;
    if (!isEmptyString(pubKeyString))
    {
        try
        {
            pubKey.setown(loadPublicKeyFromMemory(pubKeyString));
        }
        catch (IException * e)
        {
            if (!exceptions)
                exceptions.setown(makeMultiException("createDigitalSignatureManagerInstanceFromKeys"));

            exceptions->append(* makeWrappedExceptionV(e, -1, "createDigitalSignatureManagerInstanceFromKeys:Cannot load public key"));
            e->Release();
        }
    }
    if (!isEmptyString(privKeyString))
    {
        try
        {
            privKey.setown(loadPrivateKeyFromMemory(privKeyString, lenPassphrase, passPhrase));
        }
        catch (IException * e)
        {
            if (!exceptions)
                exceptions.setown(makeMultiException("createDigitalSignatureManagerInstanceFromKeys"));

            exceptions->append(* makeWrappedExceptionV(e, -1, "createDigitalSignatureManagerInstanceFromKeys:Cannot load private key"));
            e->Release();
        }
    }

    // NB: allow it continue if 1 of the keys successfully loaded.
    if (exceptions && exceptions->ordinality())
    {
        if (!pubKey && !privKey)
            throw exceptions.getClear();
        else
            EXCLOG(exceptions, nullptr);
    }
    return new CDigitalSignatureManager(pubKey, privKey);
#else
    return nullptr;
#endif
}

//Create using preloaded keys
//Caller must release when no longer needed
IDigitalSignatureManager * createDigitalSignatureManagerInstanceFromKeys(CLoadedKey *pubKey, CLoadedKey *privKey)
{
#if defined(_USE_OPENSSL)
    return new CDigitalSignatureManager(pubKey, privKey);
#else
    return nullptr;
#endif
}

} // namespace cryptohelper
