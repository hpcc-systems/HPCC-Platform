/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.

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

#ifndef APUSHARED_H
#define APUSHARED_H

#include "jlib.hpp"
#include "jstring.hpp"
#include "jutil.hpp"
#include "dynshared.hpp"

#define APRUTIL "aprutil-1"

typedef int (*apr_generate_random_bytes_t)(unsigned char*, size_t);
typedef int (*apr_base64_encode_len_t)(int);
typedef int (*apr_base64_encode_t)(char*, const char*, int);
typedef int (*apr_md5_encode_t)(const char*, const char*, char*, size_t);
typedef int (*apr_password_validate_t)(const char*, const char*);
typedef char* (*apr_cpystrn_t)(char*, const char*, size_t);

class ApuShared : public CInterface, public DynInit
{
public:
    IMPLEMENT_IINTERFACE;
    ApuShared();
    void init();
    virtual ~ApuShared()
    {
        delete so;
    }

    int apr_generate_random_bytes(unsigned char* a1, size_t a2);
    int apr_base64_encode_len(int a1);
    int apr_base64_encode(char* a1, const char* a2, int a3);
    int apr_md5_encode(const char* a1, const char* a2, char* a3, size_t a4);
    int apr_password_validate(const char* a1, const char* a2);
    char* apr_cpystrn(char* a1, const char* a2, size_t a3);

private:
    SharedObject *so;
    HINSTANCE hndl;
    bool init_s;
    StringBuffer lib_name;
    Owned<SharedObjectFunction<apr_generate_random_bytes_t> > _generate_random_bytes;
    Owned<SharedObjectFunction<apr_base64_encode_len_t> > _base64_encode_len;
    Owned<SharedObjectFunction<apr_base64_encode_t> > _base64_encode;
    Owned<SharedObjectFunction<apr_md5_encode_t> > _md5_encode;
    Owned<SharedObjectFunction<apr_password_validate_t> > _password_validate;
    Owned<SharedObjectFunction<apr_cpystrn_t> > _cpystrn;

};

#endif
