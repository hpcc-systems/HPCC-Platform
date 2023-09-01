/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

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


#ifndef JSECRETS_HPP
#define JSECRETS_HPP

#include "jlib.hpp"
#include "jstring.hpp"

interface ISecret : extends IInterface
{
    virtual const IPropertyTree * getTree() const = 0;
    virtual bool getKeyValue(MemoryBuffer & result, const char * key) const = 0;
    virtual bool getKeyValue(StringBuffer & result, const char * key) const = 0;
    virtual bool isStale() const = 0;
    //Return a sequence number which changes whenever the secret actually changes - so that a caller can determine
    //whether it needs to reload the certificates.
    virtual unsigned getVersion() const = 0;
};

extern jlib_decl void setSecretMount(const char * path);
extern jlib_decl void setSecretTimeout(unsigned timeoutMs);

extern jlib_decl IPropertyTree *getSecret(const char *category, const char * name, const char * optVaultId = nullptr, const char * optVersion = nullptr);
extern jlib_decl ISecret * resolveSecret(const char *category, const char * name, const char * optRequiredVault);

extern jlib_decl bool getSecretKeyValue(MemoryBuffer & result, const IPropertyTree *secret, const char * key);
extern jlib_decl bool getSecretKeyValue(StringBuffer & result, const IPropertyTree *secret, const char * key);
extern jlib_decl bool getSecretValue(StringBuffer & result, const char *category, const char * name, const char * key, bool required);

extern jlib_decl void initSecretUdpKey();
extern jlib_decl const MemoryAttr &getSecretUdpKey(bool required);

extern jlib_decl bool containsEmbeddedKey(const char *certificate);

extern jlib_decl IPropertyTree *queryTlsSecretInfo(const char *issuer);
extern jlib_decl IPropertyTree *createTlsClientSecretInfo(const char *issuer, bool mutual, bool acceptSelfSigned, bool addCACert=true);

extern jlib_decl  void splitFullUrl(const char *url, bool &https, StringBuffer &user, StringBuffer &password, StringBuffer &host, StringBuffer &port, StringBuffer &fullpath);
extern jlib_decl void splitUrlSchemeHostPort(const char *url, StringBuffer &user, StringBuffer &password, StringBuffer &schemeHostPort, StringBuffer &path);
extern jlib_decl void splitUrlIsolateScheme(const char *url, StringBuffer &user, StringBuffer &password, StringBuffer &scheme, StringBuffer &hostPort, StringBuffer &path);
extern jlib_decl StringBuffer &generateDynamicUrlSecretName(StringBuffer &secretName, const char *scheme, const char *userPasswordPair, const char *host, unsigned port, const char *path);
extern jlib_decl StringBuffer &generateDynamicUrlSecretName(StringBuffer &secretName, const char *url, const char *username);

extern jlib_decl bool queryMtls();

#endif
