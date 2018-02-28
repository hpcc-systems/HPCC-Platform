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

#ifndef DIGISIGN_API

#ifndef DIGISIGN_EXPORTS
    #define DIGISIGN_API DECL_IMPORT
#else
    #define DIGISIGN_API DECL_EXPORT
#endif //DIGISIGN_EXPORTS

#endif

//General purpose digital signature manager
//Useful to sign a text string, so the consumer can be assured it has not been altered
//Uses the HPCCPublicKey/HPCCPrivateKey key files specified in environment.conf
interface IDigitalSignatureManager //Public/Private key message signer/verifyer
{
public:
    virtual bool isDigiSignerConfigured() = 0;
    virtual bool isDigiVerifierConfigured() = 0;
    virtual bool digiSign(const char * text, StringBuffer & b64Signature) = 0;
    virtual bool digiVerify(const char * text, StringBuffer & b64Signature) = 0;
};

extern "C"
{
    DIGISIGN_API IDigitalSignatureManager * digitalSignatureManagerInstance();
}

#endif

