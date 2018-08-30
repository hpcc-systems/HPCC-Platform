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
#ifndef DIGISIGN_HPP
#define DIGISIGN_HPP

#ifndef CRYPTOHELPER_API

#ifndef CRYPTOHELPER_EXPORTS
    #define CRYPTOHELPER_API DECL_IMPORT
#else
    #define CRYPTOHELPER_API DECL_EXPORT
#endif //CRYPTOHELPER_EXPORTS

#endif

namespace cryptohelper
{

//General purpose digital signature manager
//Useful to sign a text string, so the consumer can be assured it has not been altered
interface IDigitalSignatureManager : extends IInterface //Public/Private key message signer/verifyer
{
public:
    virtual bool isDigiSignerConfigured() = 0;
    virtual bool isDigiVerifierConfigured() = 0;
    virtual void setKeyFileNames(const char * pub, const char * priv) = 0;
    virtual const char * queryPublicKeyFile() const = 0;
    virtual const char * queryPrivateKeyFile() const = 0;
    virtual bool digiSign(const char * text, StringBuffer & b64Signature) = 0;//signs, using private key
    virtual bool digiVerify(const char * text, StringBuffer & b64Signature) = 0;//verifies, using public key
};

extern "C"
{
    //Uses the HPCCPublicKey/HPCCPrivateKey key files specified in environment.conf
    CRYPTOHELPER_API IDigitalSignatureManager * queryDigitalSignatureManagerInstanceFromEnv();

    //Create using the given key files
    CRYPTOHELPER_API IDigitalSignatureManager * createDigitalSignatureManagerInstanceFromFiles(const char * _pubKey, const char *_privKey, const char * _passPhrase);

    //Create using the given PEM formatted keys
    CRYPTOHELPER_API IDigitalSignatureManager * createDigitalSignatureManagerInstanceFromKeys(StringBuffer & _pubKeyBuff, StringBuffer & _privKeyBuff, const char * _passPhrase);
}

} // namespace cryptohelper

#endif

