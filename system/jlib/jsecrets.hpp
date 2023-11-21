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

interface ISyncedPropertyTree;

extern jlib_decl void setSecretMount(const char * path);
extern jlib_decl void setSecretTimeout(unsigned timeoutMs);

//Return the current (cached) value of a secret.  If the secret is not defined, return nullptr.
extern jlib_decl const IPropertyTree *getSecret(const char *category, const char * name, const char * optVaultId = nullptr, const char * optVersion = nullptr);
// resolveSecret() always returns an object, which will potentially be updated behind the scenes.  If no secret is originally
// defined, but it then added to a vault or Kubernetes secret, it will then be picked up when the cache entry is
// refreshed - allowing missing configuration to be updated for a live system.
extern jlib_decl ISyncedPropertyTree * resolveSecret(const char *category, const char * name, const char * optRequiredVault, const char* optVersion);

extern jlib_decl bool getSecretKeyValue(MemoryBuffer & result, const IPropertyTree *secret, const char * key);
extern jlib_decl bool getSecretKeyValue(StringBuffer & result, const IPropertyTree *secret, const char * key);
extern jlib_decl bool getSecretValue(StringBuffer & result, const char *category, const char * name, const char * key, bool required);

extern jlib_decl void initSecretUdpKey();
extern jlib_decl const MemoryAttr &getSecretUdpKey(bool required);

extern jlib_decl bool containsEmbeddedKey(const char *certificate);

//getIssuerTlsConfig must return owned because the internal cache could be updated internally and the return will become invalid, so must be linked
extern jlib_decl const ISyncedPropertyTree * getIssuerTlsSyncedConfig(const char * issuer, const char * optTrustedPeers, bool disableMTLS);
inline const ISyncedPropertyTree * getIssuerTlsSyncedConfig(const char * issuer) { return getIssuerTlsSyncedConfig(issuer, nullptr, false); }

extern jlib_decl bool hasIssuerTlsConfig(const char *issuer);

extern jlib_decl ISyncedPropertyTree * createIssuerTlsConfig(const char * issuer, const char * optTrustedPeers, bool isClientConnection, bool acceptSelfSigned, bool addCACert, bool disableMTLS);
extern jlib_decl ISyncedPropertyTree * createStorageTlsConfig(const char * secretName, bool addCACert);

extern jlib_decl  void splitFullUrl(const char *url, bool &https, StringBuffer &user, StringBuffer &password, StringBuffer &host, StringBuffer &port, StringBuffer &fullpath);
extern jlib_decl void splitUrlSchemeHostPort(const char *url, StringBuffer &user, StringBuffer &password, StringBuffer &schemeHostPort, StringBuffer &path);
extern jlib_decl void splitUrlIsolateScheme(const char *url, StringBuffer &user, StringBuffer &password, StringBuffer &scheme, StringBuffer &host, StringBuffer &port, StringBuffer &path);
extern jlib_decl StringBuffer &generateDynamicUrlSecretName(StringBuffer &secretName, const char *scheme, const char *userPasswordPair, const char *host, unsigned port, const char *path);
extern jlib_decl StringBuffer &generateDynamicUrlSecretName(StringBuffer &secretName, const char *url, const char *username);

extern jlib_decl bool queryMtls();

#endif
