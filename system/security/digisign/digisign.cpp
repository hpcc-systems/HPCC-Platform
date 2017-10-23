/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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
#include "build-config.h"
#include <openssl/pem.h>
#include <openssl/err.h>
#include "digisign.hpp"

static CriticalSection digiSignCrit;
static CriticalSection digiVerifyCrit;



class _CDigitalSignatureManager : implements IDigitalSignatureManager
{
private:
    StringAttr   publicKeyFile;
    StringAttr   privateKeyFile;
    StringBuffer publicKeyBuff;
    StringBuffer privateKeyBuff;
    bool         signingConfigured;
    bool         verifyingConfigured;

public:
    _CDigitalSignatureManager()
    {
        signingConfigured = false;
        verifyingConfigured = false;
#ifdef _USE_ZLIB
        //query private key file location from environment.conf
        StringBuffer configFileSpec;
        configFileSpec.set(CONFIG_DIR).append(PATHSEPSTR).append("environment.conf");
        Owned<IProperties> conf = createProperties(configFileSpec.str(), true);
        if (conf)
        {
            publicKeyFile.set(conf->queryProp("HPCCPublicKey"));
            privateKeyFile.set(conf->queryProp("HPCCPrivateKey"));
            signingConfigured = !publicKeyFile.isEmpty();
            verifyingConfigured = !privateKeyFile.isEmpty();
        }
#else
        WARNLOG("CDigitalSignatureManager: Platform built without ZLIB!");
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
        {
            privateKeyBuff.loadFile(privateKeyFile.str());
            if (privateKeyBuff.isEmpty())
                throw MakeStringException(-1, "digiSign:Cannot load private key file");
        }
#ifdef _USE_ZLIB
        //create an RSA object from private key
        BIO * keybio = BIO_new_mem_buf((void*) privateKeyBuff.str(), -1);
        if (keybio == nullptr)
        {
            char buff[120];
            ERR_error_string(ERR_get_error(), buff);
            throw MakeStringException(-1, "digiSign:BIO_new_mem_buf: %s", buff);
        }

        RSA * rsa = PEM_read_bio_RSAPrivateKey(keybio, nullptr, nullptr, nullptr);
        if (rsa == nullptr)
        {
            char buff[120];
            ERR_error_string(ERR_get_error(), buff);
            throw MakeStringException(-1,
                    "digiSign:PEM_read_bio_RSAPrivateKey: %s", buff);
        }
        BIO_free_all(keybio);

        //With the RSA object, create the digest and digital signature
        EVP_MD_CTX * RSASignCtx = EVP_MD_CTX_create(); //allocate, initializes and return a digest context
        EVP_PKEY * priKey = EVP_PKEY_new();
        EVP_PKEY_assign_RSA(priKey, rsa);

        //initialize context for SHA-256 hashing function
        if (EVP_DigestSignInit(RSASignCtx, nullptr, EVP_sha256(), nullptr, priKey) <= 0)
        {
            char buff[120];
            ERR_error_string(ERR_get_error(), buff);
            throw MakeStringException(-1, "digiSign:EVP_DigestSignInit: %s", buff);
        }

        //add string to the context
        if (EVP_DigestSignUpdate(RSASignCtx, (size_t*)text, strlen(text)) <= 0)
        {
            char buff[120];
            ERR_error_string(ERR_get_error(), buff);
            throw MakeStringException(-1, "digiSign:EVP_DigestSignUpdate: %s", buff);
        }

        //compute length of signature
        size_t encMsgLen;
        if (EVP_DigestSignFinal(RSASignCtx, nullptr, &encMsgLen) <= 0)
        {
            char buff[120];
            ERR_error_string(ERR_get_error(), buff);
            throw MakeStringException(-1, "digiSign:EVP_DigestSignFinal1: %s", buff);
        }

        //compute signature (signed digest)
        unsigned char * encMsg = (unsigned char*) malloc(encMsgLen);
        if (EVP_DigestSignFinal(RSASignCtx, encMsg, &encMsgLen) <= 0)
        {
            char buff[120];
            ERR_error_string(ERR_get_error(), buff);
            throw MakeStringException(-1, "digiSign:EVP_DigestSignFinal2: %s", buff);
        }

        //convert to base64
        JBASE64_Encode(encMsg, encMsgLen, b64Signature, false);

        //cleanup
        free(encMsg);
        EVP_PKEY_free(priKey);
        EVP_MD_CTX_destroy(RSASignCtx);
        return true;//success
#else
        throw MakeStringException(-1, "digiSign:Platform built without ZLIB");
        return false;
#endif
    }


    //Verify the given text was used to create the given digital signature
    bool digiVerify(const char * text, const char * b64Signature)
    {
        if (!verifyingConfigured)
            throw MakeStringException(-1, "digiVerify:Verifying Digital Signatures not configured");

        CriticalBlock b(digiVerifyCrit);

        if (publicKeyBuff.isEmpty())
        {
            publicKeyBuff.loadFile(publicKeyFile.str());
            if (publicKeyBuff.isEmpty())
                throw MakeStringException(-1, "digiSign:Cannot load public key file");
        }
#ifdef _USE_ZLIB
        BIO * keybio = BIO_new_mem_buf((void*) publicKeyBuff.str(), -1);
        if (keybio == nullptr)
        {
            char buff[120];
            ERR_error_string(ERR_get_error(), buff);
            throw MakeStringException(-1, "digiVerify:BIO_new_mem_buf: %s", buff);
        }
        RSA * rsa = PEM_read_bio_RSA_PUBKEY(keybio, nullptr, nullptr, nullptr);

        EVP_PKEY* pubKey = EVP_PKEY_new();
        EVP_PKEY_assign_RSA(pubKey, rsa);
        EVP_MD_CTX * RSAVerifyCtx = EVP_MD_CTX_create();//allocate, initializes and return a digest context
        if (EVP_DigestVerifyInit(RSAVerifyCtx, nullptr, EVP_sha256(), nullptr, pubKey) <= 0)
        {
            char buff[120];
            ERR_error_string(ERR_get_error(), buff);
            throw MakeStringException(-1, "digiVerify:EVP_DigestVerifyInit: %s", buff);
        }
        BIO_free_all(keybio);

        //decode base64 signature
        StringBuffer decodedSig;
        JBASE64_Decode(b64Signature, decodedSig);
        if (EVP_DigestVerifyUpdate(RSAVerifyCtx, text, strlen(text)) <= 0)
        {
            char buff[120];
            ERR_error_string(ERR_get_error(), buff);
            throw MakeStringException(-1, "digiVerify:EVP_DigestVerifyUpdate: %s", buff);
        }

        int match = EVP_DigestVerifyFinal(RSAVerifyCtx, (const unsigned char *)decodedSig.str(), decodedSig.length());
        EVP_PKEY_free(pubKey);
        EVP_MD_CTX_destroy(RSAVerifyCtx);//cleans up digest context ctx
        return match == 1;
#else
        throw MakeStringException(-1, "digiSign:Platform built without ZLIB");
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


