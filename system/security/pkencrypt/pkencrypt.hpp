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
#ifndef PKENCRYPTIONMGR_HPP
#define PKENCRYPTIONMGR_HPP

#ifndef PKENCRYPTIONMGR_API

#ifndef PKENCRYPTIONMGR_EXPORTS
    #define PKENCRYPTIONMGR_API DECL_IMPORT
#else
    #define PKENCRYPTIONMGR_API DECL_EXPORT
#endif //PKENCRYPTIONMGR_EXPORTS

#endif

//General purpose public key encrypter, private key decrypter
//Creates and extracts workunit security token
//Uses the public/private key files specified in environment.conf
interface IPKEncryptionManager //Public/Private key encryption manager
{
public:	
    virtual bool isEncrypterConfigured()=0;
    virtual bool isDecrypterConfigured()=0;
    virtual bool PKEncrypt(const char * _src, StringAttr & _key, StringAttr & _token)=0;
    virtual bool PKDecrypt(const char * _key, const char * _token, StringAttr & _result)=0;
    virtual bool createWUSecurityToken(const char * _wuid, const char * _user, const char * _password, IStringVal & _key, IStringVal & _token)=0;
    virtual bool extractWUSecurityToken(const char * _key, const char * _token, const char * _wuid, IStringVal & _user, IStringVal & _password)=0;
};

extern "C"
{
    PKENCRYPTIONMGR_API IPKEncryptionManager * PKEncryptionManagerInstance();
}

#endif

