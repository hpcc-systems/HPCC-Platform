/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#include "apushared.hpp"
#include "jlib.hpp"
#include "jstring.hpp"
#include "jexcept.hpp"
#include "jutil.hpp"
#include "dynshared.hpp"

ApuShared::ApuShared()
{
    setName("ApuShared");
    lib_name.append(SharedObjectPrefix).append(APRUTIL).append(SharedObjectExtension);
    so = new SharedObject();
    setInit(false);
}

void ApuShared::init()
{
    if( !so->load(lib_name, true) )
    {
        throw MakeStringException(-1, "load %s failed with code %d", lib_name.str(), GetSharedObjectError());
    }
    hndl = so->getInstanceHandle();
    _generate_random_bytes.setown(new SharedObjectFunction<apr_generate_random_bytes_t>(hndl, "apr_generate_random_bytes"));
    _base64_encode_len.setown(new SharedObjectFunction<apr_base64_encode_len_t>(hndl, "apr_base64_encode_len"));
    _base64_encode.setown(new SharedObjectFunction<apr_base64_encode_t>(hndl, "apr_base64_encode"));
    _md5_encode.setown(new SharedObjectFunction<apr_md5_encode_t>(hndl, "apr_md5_encode"));
    _password_validate.setown(new SharedObjectFunction<apr_password_validate_t>(hndl, "apr_password_validate"));
    _cpystrn.setown(new SharedObjectFunction<apr_cpystrn_t>(hndl, "apr_cpystrn"));
    setInit(true);
}

int ApuShared::apr_generate_random_bytes(unsigned char* a1, size_t a2)
{
    checkInit();
    return (_generate_random_bytes->getFuncPtr())(a1, a2);
}

int ApuShared::apr_base64_encode_len(int a1)
{
    checkInit();
    return (_base64_encode_len->getFuncPtr())(a1);
}

int ApuShared::apr_base64_encode(char* a1, const char* a2, int a3)
{
    checkInit();
    return (_base64_encode->getFuncPtr())(a1,a2,a3);
}

int ApuShared::apr_md5_encode(const char* a1, const char* a2, char* a3, size_t a4)
{
    checkInit();
    return (_md5_encode->getFuncPtr())(a1,a2,a3,a4);
}

int ApuShared::apr_password_validate(const char* a1, const char* a2)
{
    checkInit();
    return (_password_validate->getFuncPtr())(a1,a2);
}

char* ApuShared::apr_cpystrn(char* a1, const char* a2, size_t a3)
{
    checkInit();
    return (_cpystrn->getFuncPtr())(a1,a2,a3);
}
