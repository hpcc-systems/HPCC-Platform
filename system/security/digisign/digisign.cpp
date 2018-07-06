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
#if defined(_USE_OPENSSL) && !defined(_WIN32)
#include <openssl/pem.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#endif
#include "jencrypt.hpp"
#include "digisign.hpp"
#include <mutex>


#if defined(_USE_OPENSSL) && !defined(_WIN32)

#define EVP_CLEANUP(key,ctx) EVP_PKEY_free(key);       \
                             EVP_MD_CTX_destroy(ctx);

#define EVP_THROW(str) {                                            \
                         char buff[120];                            \
                         ERR_error_string(ERR_get_error(), buff);   \
                         throw MakeStringException(-1, str, buff);  \
                       }

class CDigitalSignatureManager : implements IDigitalSignatureManager, public CInterface
{
private:
    StringBuffer publicKeyBuff;
    StringBuffer privateKeyBuff;
    StringBuffer passphraseBuffEnc;
    bool         signingConfigured;
    bool         verifyingConfigured;

    bool digiInit(bool isSigning, const char * passphraseEnc, EVP_MD_CTX * * ctx, EVP_PKEY * * PKey)
    {
        //To avoid threading issues, the keys are created on each call. Otherwise would require
        //serialization with a critical section or implementing locking callbacks
        const char * keyBuff = isSigning ? privateKeyBuff.str() : publicKeyBuff.str();

        //create an RSA object from public key
        BIO * keybio = BIO_new_mem_buf((void*) keyBuff, -1);
        if (nullptr == keybio)
        {
            EVP_THROW("digiSign:BIO_new_mem_buf: %s");
        }

        RSA * rsa;
        if (isSigning)
        {
            StringBuffer ppDec;
            if (!isEmptyString(passphraseEnc))
                decrypt(ppDec, passphraseEnc);
            rsa = PEM_read_bio_RSAPrivateKey(keybio, nullptr, nullptr, (void*)ppDec.str());
        }
        else
            rsa = PEM_read_bio_RSA_PUBKEY(keybio, nullptr, nullptr, nullptr);
        BIO_free_all(keybio);
        if (nullptr == rsa)
        {
            if (isSigning)
                EVP_THROW("digiSign:PEM_read_bio_RSAPrivateKey: %s")
            else
                EVP_THROW("digiSign:PEM_read_bio_RSA_PUBKEY: %s")
        }

        EVP_PKEY* pKey = EVP_PKEY_new();
        if (nullptr == pKey)
        {
            RSA_free(rsa);
            EVP_THROW("digiSign:EVP_PKEY_new: %s");
        }
        EVP_PKEY_assign_RSA(pKey, rsa);//take ownership of the rsa. pKey will free rsa

        EVP_MD_CTX * RSACtx = EVP_MD_CTX_create();//allocate, initializes and return a digest context
        if (nullptr == RSACtx)
        {
            EVP_PKEY_free(pKey);
            EVP_THROW("digiSign:EVP_MD_CTX_create: %s");
        }

        //initialize context for SHA-256 hashing function
        int rc;
        if (isSigning)
            rc = EVP_DigestSignInit(RSACtx, nullptr, EVP_sha256(), nullptr, pKey);
        else
            rc = EVP_DigestVerifyInit(RSACtx, nullptr, EVP_sha256(), nullptr, pKey);
        if (rc <= 0)
        {
            EVP_CLEANUP(pKey, RSACtx);//cleans allocated key and digest context
            if (isSigning)
                EVP_THROW("digiSign:EVP_DigestSignInit: %s")
            else
                EVP_THROW("digiSign:EVP_DigestVerifyInit: %s")
        }
        *ctx = RSACtx;
        *PKey = pKey;
        return true;
    }

public:
    IMPLEMENT_IINTERFACE;

    CDigitalSignatureManager(const char * _pubKeyBuff, const char * _privKeyBuff, const char * _passPhrase)
        : signingConfigured(false), verifyingConfigured(false)
    {
        publicKeyBuff.set(_pubKeyBuff);
        privateKeyBuff.set(_privKeyBuff);
        passphraseBuffEnc.set(_passPhrase);//MD5 encrypted passphrase
        signingConfigured = !publicKeyBuff.isEmpty();
        verifyingConfigured = !privateKeyBuff.isEmpty();
    }

    bool isDigiSignerConfigured()
    {
        return signingConfigured;
    }

    bool isDigiVerifierConfigured()
    {
        return verifyingConfigured;
    }

    //Create base 64 encoded digital signature of given text string
    bool digiSign(const char * text, StringBuffer & b64Signature)
    {
        if (!signingConfigured)
            throw MakeStringException(-1, "digiSign:Creating Digital Signatures not configured");

        EVP_MD_CTX * signingCtx;
        EVP_PKEY *   signingKey;
        digiInit(true, passphraseBuffEnc.str(), &signingCtx, &signingKey);

        //add string to the context
        if (EVP_DigestSignUpdate(signingCtx, (size_t*)text, strlen(text)) <= 0)
        {
            EVP_CLEANUP(signingKey, signingCtx);
            EVP_THROW("digiSign:EVP_DigestSignUpdate: %s");
        }

        //compute length of signature
        size_t encMsgLen;
        if (EVP_DigestSignFinal(signingCtx, nullptr, &encMsgLen) <= 0)
        {
            EVP_CLEANUP(signingKey, signingCtx);
            EVP_THROW("digiSign:EVP_DigestSignFinal1: %s");
        }

        if (encMsgLen == 0)
        {
            EVP_CLEANUP(signingKey, signingCtx);
            EVP_THROW("digiSign:EVP_DigestSignFinal length returned 0: %s");
        }

        //compute signature (signed digest)
        unsigned char * encMsg = (unsigned char*) malloc(encMsgLen);
        if (encMsg == nullptr)
        {
            EVP_CLEANUP(signingKey, signingCtx);
            throw MakeStringException(-1, "digiSign:malloc(%u) returned NULL",(unsigned)encMsgLen);
        }

        if (EVP_DigestSignFinal(signingCtx, encMsg, &encMsgLen) <= 0)
        {
            free(encMsg);
            EVP_CLEANUP(signingKey, signingCtx);
            EVP_THROW("digiSign:EVP_DigestSignFinal2: %s");
        }


        //convert to base64
        JBASE64_Encode(encMsg, encMsgLen, b64Signature, false);

        //cleanup
        free(encMsg);
        EVP_CLEANUP(signingKey, signingCtx);
        return true;//success
    }


    //Verify the given text was used to create the given digital signature
    bool digiVerify(const char * text, StringBuffer & b64Signature)
    {
        if (!verifyingConfigured)
            throw MakeStringException(-1, "digiVerify:Verifying Digital Signatures not configured");

        EVP_MD_CTX * verifyingCtx;
        EVP_PKEY *   verifyingKey;
        digiInit(false, passphraseBuffEnc.str(), &verifyingCtx, &verifyingKey);

        //decode base64 signature
        StringBuffer decodedSig;
        JBASE64_Decode(b64Signature.str(), decodedSig);

        if (EVP_DigestVerifyUpdate(verifyingCtx, text, strlen(text)) <= 0)
        {
            EVP_CLEANUP(verifyingKey, verifyingCtx);
            EVP_THROW("digiVerify:EVP_DigestVerifyUpdate: %s");
        }

        int match = EVP_DigestVerifyFinal(verifyingCtx, (unsigned char *)decodedSig.str(), decodedSig.length());
        EVP_CLEANUP(verifyingKey, verifyingCtx);
        return match == 1;
    }
};

#else

//Dummy implementation if no OPENSSL available.
class CDigitalSignatureManager : implements IDigitalSignatureManager, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;

    CDigitalSignatureManager(const char * _pubKeyBuff, const char * _privKeyBuff, const char * _passPhrase)
    {
        WARNLOG("CDigitalSignatureManager: Platform built without OPENSSL!");
    }

    bool isDigiSignerConfigured()
    {
        return false;
    }

    bool isDigiVerifierConfigured()
    {
        return false;
    }

    //Create base 64 encoded digital signature of given text string
    bool digiSign(const char * text, StringBuffer & b64Signature)
    {
        //convert to base64
        JBASE64_Encode(text, strlen(text), b64Signature, false);
        return true;//success
    }

    //Verify the given text was used to create the given digital signature
    bool digiVerify(const char * text, StringBuffer & b64Signature)
    {
        //decode base64 signature
        StringBuffer decodedSig;
        JBASE64_Decode(b64Signature.str(), decodedSig);
        return streq(text, decodedSig);
    }
};

#endif


static IDigitalSignatureManager * dsm;
static std::once_flag dsmInitFlag;
static std::once_flag dsmAddAlgoFlag;

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}
MODULE_EXIT()
{
    ::Release(dsm);
}

static void createDigitalSignatureManagerInstance(IDigitalSignatureManager * * ppDSM)
{
    const char * pubKey = nullptr, *privKey = nullptr, *passPhrase = nullptr;
    queryHPCCPKIKeyFiles(nullptr, &pubKey, &privKey, &passPhrase);
    *ppDSM = createDigitalSignatureManagerInstanceFromFiles(pubKey, privKey, passPhrase);
}

static void addAlgorithms()
{
#if defined(_USE_OPENSSL) && !defined(_WIN32)
    OpenSSL_add_all_algorithms();
#endif
}

extern "C"
{
    //Returns reference to singleton instance created from environment.conf key file settings
    DIGISIGN_API IDigitalSignatureManager * queryDigitalSignatureManagerInstanceFromEnv()
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
    DIGISIGN_API IDigitalSignatureManager * createDigitalSignatureManagerInstanceFromFiles(const char * _pubKey, const char *_privKey, const char * _passPhrase)
    {
#if defined(_USE_OPENSSL) && !defined(_WIN32)
        StringBuffer privateKeyBuff;
        StringBuffer publicKeyBuff;

        if (!isEmptyString(_pubKey))
        {
            try
            {
                publicKeyBuff.loadFile(_pubKey);
            }
            catch (IException * e)
            {
                e->Release();
            }
            if (publicKeyBuff.isEmpty())
                throw MakeStringException(-1, "digiSign:Cannot load public key file");
        }

        if (!isEmptyString(_privKey))
        {
            try
            {
                privateKeyBuff.loadFile(_privKey);
            }
            catch (IException * e)
            {
                e->Release();
            }
            if (privateKeyBuff.isEmpty())
                throw MakeStringException(-1, "digiSign:Cannot load private key file");
        }

        return createDigitalSignatureManagerInstanceFromKeys(publicKeyBuff, privateKeyBuff, _passPhrase);
#else
        return nullptr;
#endif
    }

    //Create using given PEM formatted keys
    //Caller must release when no longer needed
    DIGISIGN_API IDigitalSignatureManager * createDigitalSignatureManagerInstanceFromKeys(StringBuffer & _pubKeyBuff, StringBuffer & _privKeyBuff, const char * _passPhrase)
    {
#if defined(_USE_OPENSSL) && !defined(_WIN32)
        std::call_once(dsmAddAlgoFlag, addAlgorithms);
        return new CDigitalSignatureManager(_pubKeyBuff, _privKeyBuff, _passPhrase);
#else
        return nullptr;
#endif
    }
}

