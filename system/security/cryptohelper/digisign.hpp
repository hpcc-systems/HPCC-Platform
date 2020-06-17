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

#include "jiface.hpp"

#include "pke.hpp"

namespace cryptohelper
{

//Create base 64 encoded digital signature of given data
jlib_decl bool digiSign(StringBuffer &b64Signature, size32_t dataSz, const void *data, const CLoadedKey &signingKey);

//Verify the given data was used to create the given digital signature
jlib_decl bool digiVerify(const char *b64Signature, size32_t dataSz, const void *data, const CLoadedKey &verifyingKey);

//General purpose digital signature manager
//Useful to sign data, so the consumer can be assured it has not been altered
interface jlib_decl IDigitalSignatureManager : extends IInterface //Public/Private key message signer/verifyer
{
public:
    virtual bool isDigiSignerConfigured() const = 0;
    virtual bool isDigiVerifierConfigured() const = 0;
    virtual bool digiSign(StringBuffer & b64Signature, size32_t dataSz, const void *data) const = 0;//signs, using private key
    virtual bool digiSign(StringBuffer & b64Signature, const char *text) const = 0;
    virtual bool digiVerify(const char *b64Signature, size32_t dataSz, const void *data) const = 0;//verifies, using public key
    virtual bool digiVerify(const char *b64Signature, const char *text) const = 0;
    virtual const char * queryKeyName() const = 0;
};

//Uses the HPCCPublicKey/HPCCPrivateKey key files specified in environment.conf
jlib_decl IDigitalSignatureManager * queryDigitalSignatureManagerInstanceFromEnv();

//Create using the given key files
jlib_decl IDigitalSignatureManager * createDigitalSignatureManagerInstanceFromFiles(const char *pubKeyFileName, const char *privKeyFileName, const char *passPhrase);
jlib_decl IDigitalSignatureManager * createDigitalSignatureManagerInstanceFromFiles(const char *pubKeyFileName, const char *privKeyFileName, size32_t lenPassphrase, const void *passPhrase);

//Create using the given PEM formatted keys
jlib_decl IDigitalSignatureManager * createDigitalSignatureManagerInstanceFromKeys(const char *pubKeyString, const char *privKeyString, const char *passPhrase);
jlib_decl IDigitalSignatureManager * createDigitalSignatureManagerInstanceFromKeys(const char *pubKeyString, const char *privKeyString, size32_t lenPassphrase, const void *passPhrase);

//Create using preloaded keys.
jlib_decl IDigitalSignatureManager * createDigitalSignatureManagerInstanceFromKeys(CLoadedKey *pubKey, CLoadedKey *privKey);

} // namespace cryptohelper

#endif // DIGISIGN_HPP

