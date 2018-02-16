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
#ifdef _USE_OPENSSL
#include <openssl/pem.h>
#include <openssl/err.h>
#endif
#include "digisign.hpp"

static CriticalSection digiSignCrit;
static CriticalSection digiVerifyCrit;

#define EVP_CLEANUP(key,ctx) EVP_PKEY_free(key);       \
                             EVP_MD_CTX_destroy(ctx);

#define EVP_THROW(str) char buff[120];                            \
                       ERR_error_string(ERR_get_error(), buff);   \
                       throw MakeStringException(-1, str, buff);


class _CDigitalSignatureManager : implements IDigitalSignatureManager
{
private:
    StringAttr   publicKeyFile;
    StringBuffer publicKeyBuff;
    StringAttr   privateKeyFile;
    StringBuffer privateKeyBuff;
    StringBuffer passphraseBuff;
    bool         signingConfigured;
    bool         verifyingConfigured;

public:
    _CDigitalSignatureManager()
    {
        signingConfigured = false;
        verifyingConfigured = false;
#ifdef _USE_OPENSSL
        //query private key file location from environment.conf
        const char * cert=0, * privKey=0, * passPhrase=0;
        bool rc = queryHPCCPKIKeyFiles(&cert, &privKey, &passPhrase);
        if (!isEmptyString(cert))
            publicKeyFile.set(cert);
        if (!isEmptyString(privKey))
            privateKeyFile.set(privKey);
        if (!isEmptyString(passPhrase))
            passphraseBuff.set(passPhrase);
        signingConfigured = !publicKeyFile.isEmpty();
        verifyingConfigured = !privateKeyFile.isEmpty();
#else
        WARNLOG("CDigitalSignatureManager: Platform built without OPENSSL!");
#endif
    }

    virtual ~_CDigitalSignatureManager()
    {
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

        CriticalBlock b(digiSignCrit);

        if (privateKeyBuff.isEmpty())
            privateKeyBuff.loadFile(privateKeyFile.str());
        if (privateKeyBuff.isEmpty())
            throw MakeStringException(-1, "digiSign:Cannot load private key file");

#ifdef _USE_OPENSSL
        //create an RSA object from private key
        BIO * keybio = BIO_new_mem_buf((void*) privateKeyBuff.str(), -1);
        if (keybio == nullptr)
        {
            EVP_THROW("digiSign:BIO_new_mem_buf: %s");
        }

        RSA * rsa = PEM_read_bio_RSAPrivateKey(keybio, nullptr, nullptr, (void*)passphraseBuff.str());
        if (rsa == nullptr)
        {
            BIO_free_all(keybio);
            EVP_THROW("digiSign:PEM_read_bio_RSAPrivateKey: %s");
        }
        BIO_free_all(keybio);

        //With the RSA object, create the digest and digital signature
        EVP_MD_CTX * RSASignCtx = EVP_MD_CTX_create(); //allocate, initializes and return a digest context
        if (nullptr == RSASignCtx)
        {
            EVP_THROW("digiSign:EVP_MD_CTX_create returned NULL: %s");
        }
        EVP_PKEY * priKey = EVP_PKEY_new();
        EVP_PKEY_assign_RSA(priKey, rsa);

        //initialize context for SHA-256 hashing function
        if (EVP_DigestSignInit(RSASignCtx, nullptr, EVP_sha256(), nullptr, priKey) <= 0)
        {
            EVP_CLEANUP(priKey, RSASignCtx);
            EVP_THROW("digiSign:EVP_DigestSignInit: %s");
        }

        //add string to the context
        if (EVP_DigestSignUpdate(RSASignCtx, (size_t*)text, strlen(text)) <= 0)
        {
            EVP_CLEANUP(priKey, RSASignCtx);
            EVP_THROW("digiSign:EVP_DigestSignUpdate: %s");
        }

        //compute length of signature
        size_t encMsgLen;
        if (EVP_DigestSignFinal(RSASignCtx, nullptr, &encMsgLen) <= 0)
        {
            EVP_CLEANUP(priKey, RSASignCtx);
            EVP_THROW("digiSign:EVP_DigestSignFinal1: %s");
        }

        if (encMsgLen == 0)
        {
            EVP_CLEANUP(priKey, RSASignCtx);
            EVP_THROW("digiSign:EVP_DigestSignFinal length returned 0: %s");
        }

        //compute signature (signed digest)
        unsigned char * encMsg = (unsigned char*) malloc(encMsgLen);
        if (encMsg == nullptr)
        {
            EVP_CLEANUP(priKey, RSASignCtx);
            throw MakeStringException(-1, "digiSign:malloc(%ld) returned NULL",encMsgLen);
        }

        if (EVP_DigestSignFinal(RSASignCtx, encMsg, &encMsgLen) <= 0)
        {
            free(encMsg);
            EVP_CLEANUP(priKey, RSASignCtx);
            EVP_THROW("digiSign:EVP_DigestSignFinal2: %s");
        }

        //convert to base64
        JBASE64_Encode(encMsg, encMsgLen, b64Signature, false);

        //cleanup
        free(encMsg);
        EVP_CLEANUP(priKey, RSASignCtx);
        return true;//success
#else
        throw MakeStringException(-1, "digiSign:Platform built without OPENSSL");
        return false;
#endif
    }


    //Verify the given text was used to create the given digital signature
    bool digiVerify(const char * text, StringBuffer & b64Signature)
    {
        if (!verifyingConfigured)
            throw MakeStringException(-1, "digiVerify:Verifying Digital Signatures not configured");

        CriticalBlock b(digiVerifyCrit);

        if (publicKeyBuff.isEmpty())
            publicKeyBuff.loadFile(publicKeyFile.str());
        if (publicKeyBuff.isEmpty())
            throw MakeStringException(-1, "digiSign:Cannot load public key file");

#ifdef _USE_OPENSSL
        BIO * keybio = BIO_new_mem_buf((void*) publicKeyBuff.str(), -1);
        if (keybio == nullptr)
        {
            EVP_THROW("digiVerify:BIO_new_mem_buf: %s");
        }
        RSA * rsa = PEM_read_bio_RSA_PUBKEY(keybio, nullptr, nullptr, nullptr);

        EVP_PKEY* pubKey = EVP_PKEY_new();
        EVP_PKEY_assign_RSA(pubKey, rsa);
        EVP_MD_CTX * RSAVerifyCtx = EVP_MD_CTX_create();//allocate, initializes and return a digest context
        if (RSAVerifyCtx == nullptr)
        {
            EVP_PKEY_free(pubKey);
            BIO_free_all(keybio);
            EVP_THROW("digiVerify:EVP_MD_CTX_create: %s");
        }

        if (EVP_DigestVerifyInit(RSAVerifyCtx, nullptr, EVP_sha256(), nullptr, pubKey) <= 0)
        {
            EVP_CLEANUP(pubKey, RSAVerifyCtx);//cleans allocated key and digest context
            BIO_free_all(keybio);
            EVP_THROW("digiVerify:EVP_DigestVerifyInit: %s");
        }
        BIO_free_all(keybio);

        //decode base64 signature
        StringBuffer decodedSig;
        JBASE64_Decode(b64Signature.str(), decodedSig);
        if (EVP_DigestVerifyUpdate(RSAVerifyCtx, text, strlen(text)) <= 0)
        {
            EVP_CLEANUP(pubKey, RSAVerifyCtx);//cleans allocated key and digest context
            EVP_THROW("digiVerify:EVP_DigestVerifyUpdate: %s");
        }

        int match = EVP_DigestVerifyFinal(RSAVerifyCtx, (unsigned char *)decodedSig.str(), decodedSig.length());
        EVP_CLEANUP(pubKey, RSAVerifyCtx);//cleans allocated key and digest context
        return match == 1;
#else
        throw MakeStringException(-1, "digiSign:Platform built without OPENSSL");
        return false;
#endif
    }
} CDigitalSignatureManager;


extern "C"
{

    DIGISIGN_API IDigitalSignatureManager * digitalSignatureManagerInstance()
    {
        return & CDigitalSignatureManager;
    }
}


