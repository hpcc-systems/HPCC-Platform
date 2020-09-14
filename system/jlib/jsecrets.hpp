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

extern jlib_decl void setSecretMount(const char * path);
extern jlib_decl void setSecretTimeout(unsigned timeoutMs);

extern jlib_decl IPropertyTree *getLocalSecret(const char *category, const char * name);
extern jlib_decl IPropertyTree *getVaultSecret(const char *category, const char *vaultId, const char * name, const char *version=nullptr);
extern jlib_decl IPropertyTree *getSecret(const char *category, const char * name);

extern jlib_decl bool getSecretKeyValue(MemoryBuffer & result, IPropertyTree *secret, const char * key);
extern jlib_decl bool getSecretKeyValue(StringBuffer & result, IPropertyTree *secret, const char * key);
extern jlib_decl bool getSecretValue(StringBuffer & result, const char *category, const char * name, const char * key, bool required);

extern jlib_decl  void splitFullUrl(const char *url, bool &https, StringBuffer &user, StringBuffer &password, StringBuffer &host, StringBuffer &port, StringBuffer &fullpath);
extern jlib_decl void splitUrlSchemeHostPort(const char *url, StringBuffer &user, StringBuffer &password, StringBuffer &schemeHostPort, StringBuffer &path);

#endif
