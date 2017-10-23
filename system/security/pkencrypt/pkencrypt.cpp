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
#include "platform.h"
#include "zcrypt.hpp"
#include "build-config.h"
#include "pkencrypt.hpp"
#include <openssl/pem.h>
#include <openssl/err.h>

//Helper to public/private key encrytption
static CriticalSection encrypterCrit;//serializes encryption
static CriticalSection decrypterCrit;//serializes decryption


class _CPKEncryptionManager : implements IPKEncryptionManager
{
private:
#ifdef _USE_ZLIB
    StringAttr   privateKeyFile;
    StringAttr   publicKeyFile;
    StringBuffer privateKey;
    IZDecryptor* pDecrypter;//single instance of decrypter, which is not reentrant
    IZEncryptor* pEncrypter;//single instance of encrypter, which is not reentrant
#endif
    bool         encrpytConfigured;
    bool         decrpytConfigured;

public:
    _CPKEncryptionManager()
    {
        encrpytConfigured = false;
        decrpytConfigured = false;
        pEncrypter = nullptr;
        pDecrypter = nullptr;
#ifdef _USE_ZLIB
        //query private key file location from environment.conf
        StringBuffer configFileSpec;
        configFileSpec.set(CONFIG_DIR).append(PATHSEPSTR).append("environment.conf");
        Owned<IProperties> conf = createProperties(configFileSpec.str(), true);
        if (conf)
        {
            publicKeyFile.set(conf->queryProp("HPCCPublicKey"));
            privateKeyFile.set(conf->queryProp("HPCCPrivateKey"));
            encrpytConfigured = !publicKeyFile.isEmpty();
            decrpytConfigured = !privateKeyFile.isEmpty();
        }
#else
        WARNLOG("PKEncrypt: Platform built without ZLIB!");
#endif
    }

    virtual ~_CPKEncryptionManager()
    {
#ifdef _USE_ZLIB
        {
            CriticalBlock b(encrypterCrit);
            if (pEncrypter)
                releaseIZ(pEncrypter);
        }

        {
            CriticalBlock b(decrypterCrit);
            if (pDecrypter)
                releaseIZ(pDecrypter);
        }
#endif
    }

    virtual inline bool isEncrypterConfigured()
    {
        return encrpytConfigured;
    }

    virtual bool PKEncrypt(const char * _str, StringAttr & _key, StringAttr & _token)
    {
        if (!encrpytConfigured)
            throw MakeStringException(-1, "PKEncrypt error: encryption not configured");

        if (_str == nullptr  || *_str == '\0')
            return false;

#ifdef _USE_ZLIB
        CriticalBlock b(encrypterCrit);

        if (!pEncrypter)
        {
            StringBuffer pubKey;
            pubKey.loadFile(publicKeyFile);//throws on error
            pEncrypter = createZEncryptor(pubKey.str());
            if (pEncrypter)
            {
                pEncrypter->setEncoding(true);
#ifdef _DEBUG
                pEncrypter->setTraceLevel(100);
#endif
            }
            else
                throw MakeStringException(-1, "PKEncrypt error: creating PKI encrypter using key file %s", publicKeyFile.str());
        }

        IZBuffer* buf = nullptr;
        IZBuffer* key = nullptr;
        int rc = pEncrypter->encrypt(strlen(_str), (unsigned char*)_str, key, buf);
        if (rc == 0)
        {
            if (buf->length())
                _token.set((const char *)buf->buffer(), buf->length());
            if (key->length())
                _key.set((const char *)key->buffer(), key->length());
            releaseIZ(buf);
            releaseIZ(key);
            return true;//success
        }
        else
            throw MakeStringException(-1, "PKEncrypt error: %d PKI encrypting token using key file %s", rc, publicKeyFile.str());
#else
        throw MakeStringException(-1, "PKEncrypt error: Platform built without ZLIB!");
#endif
        return false;//fail
    }

    virtual inline bool isDecrypterConfigured()
    {
        return decrpytConfigured;
    }

    virtual bool PKDecrypt(const char * _key, const char * _token, StringAttr & _result)
    {
        if (!decrpytConfigured)
            throw MakeStringException(-1, "PKEncrypt error: decryption not enabled");

        if (_key == nullptr || *_key == '\0')
            return false;

#ifdef _USE_ZLIB
        CriticalBlock b(decrypterCrit);

        if (!pDecrypter)
        {
            StringBuffer privateKeyBuff;
            privateKeyBuff.loadFile(privateKeyFile.str());
            if (privateKeyBuff.isEmpty())
                throw MakeStringException(-1, "PKEncrypt error: Could not read private key file");
            pDecrypter = createZDecryptor(privateKeyBuff.str(), nullptr);//create once
            if (pDecrypter)
            {
                pDecrypter->setEncoding(true);
#ifdef _DEBUG
                pDecrypter->setTraceLevel(100);
#endif
            }
            else
                throw MakeStringException(-1, "PKEncrypt error: could not create IZDecryptor");
        }

        unsigned char * pKey = (unsigned char *)_key;
        unsigned char * pToken = (unsigned char *)_token;
        IZBuffer* result = pDecrypter->decrypt(pKey, pToken);
        if (result)
        {
            _result.set((char*)result->buffer(), result->length());
            releaseIZ(result);
            return true;  //success
        }
        else
            throw MakeStringException(-1, "PKEncrypt error: decrypter return NULL");
#else
        WARNLOG("PKEncrypt error:: Platform built without ZLIB!");
#endif
        return false;//failure
    }


    //encrypt workunit security token
    //Format is WUID;USER;PASSWORD
    virtual bool createWUSecurityToken(const char * _wuid, const char * _user, const char * _password, IStringVal & _key, IStringVal & _token)
    {
        if (!encrpytConfigured)
            throw MakeStringException(-1, "PKEncrypt error: encryption not enabled");
#ifdef _USE_ZLIB
        VStringBuffer str("%s;%s;%s", _wuid, _user, _password);
        StringAttr tokStr(str);
        StringAttr key;
        StringAttr token;
        bool rc = PKEncrypt(str, key, token);
        if (rc)
        {
            _key.set(key);
            _token.set(token);
            return true;//success
        }
        else
            throw MakeStringException(-1, "PKEncrypt error: %d PKI encrypting token using key file %s", rc, publicKeyFile.str());
#else
        throw MakeStringException(-1, "PKEncrypt error: Platform built without ZLIB!");
#endif
        return false;//fail
    }

    //decrypt and tokenize workunit security token
    //Format is WUID;USER;PASSWORD
    virtual bool extractWUSecurityToken(const char * _key, const char * _token, const char * _wuid, IStringVal & _user, IStringVal & _password)
    {
        if (!decrpytConfigured)
            throw MakeStringException(-1, "PKEncrypt error: decryption not configured");
#ifdef _USE_ZLIB
        StringAttr str;
        bool ok = PKDecrypt(_key, _token, str);
        if (ok)
        {
            char * pBuffer = (char*)str.str();
            const char * finger = strchr(pBuffer, ';');
            bool wuidsMatch = _wuid ? (finger && (0 == strnicmp(_wuid, pBuffer, finger - pBuffer))) : true;
            if (finger && wuidsMatch)
            {
                const char * finger1 = strchr(++finger, ';');
                if (finger1)
                {
                    _user.setLen(finger, (size32_t) (finger1 - finger));
                    finger1++;
                    _password.setLen(finger1, (size32_t) (pBuffer + str.length() - finger1));
                    return true;  //success
                }
            }
            throw MakeStringException(-1, "PKEncrypt error: Invalid call to extractWUSecurityToken");
        }
        else
            throw MakeStringException(-1, "PKEncrypt error: decrypter return NULL");
#else
        WARNLOG("PKEncrypt error: Platform built without ZLIB!");
#endif
        return false;//failure
    }
} CPKEncryptionManager;


extern "C"
{

    PKENCRYPTIONMGR_API IPKEncryptionManager * PKEncryptionManagerInstance()
    {
        return & CPKEncryptionManager;
    }
}

