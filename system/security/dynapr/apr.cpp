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

#include "apr.hpp"

#include "jlib.hpp"
#include "jstring.hpp"
#include "dynshared.hpp"
#include "aprshared.hpp"
#include "apushared.hpp"

#define MAX_STRING_LEN 256
#define SALT_LEN 9

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    newAprObject();
    return true;
}

MODULE_EXIT()
{
    destroyAprObject();
}

Apr::Apr()
{
    setName("Apr");
    setInit(false);
}

Apr::~Apr()
{
    if (queryInit())
        apr->apr_terminate();
}

void Apr::init()
{
    AprShared *apr_s = new AprShared();
    ApuShared *apu_s = new ApuShared();
    apr_s->init();
    apu_s->init();
    apr.setown(apr_s);
    apu.setown(apu_s);
    apr->apr_initialize();
    setInit(true);
}

void Apr::apr_md5_string(StringBuffer& inpstring, StringBuffer& outstring)
{
    checkInit();
    char salt[SALT_LEN];
    char saltedAprMd5[MAX_STRING_LEN];
    rand_salt(salt);
    apu->apr_md5_encode(inpstring.str(), salt, saltedAprMd5, sizeof(saltedAprMd5));
    outstring.append(saltedAprMd5);
}

apr_ret_t Apr::apr_md5_validate(StringBuffer& inpstring, StringBuffer& aprmd5string)
{
    checkInit();
    int result;
    result = apu->apr_password_validate(inpstring.str(), aprmd5string.str());
    if (result == 0)
    {
        return APR_MD5_TRUE;
    }
    else
    {
        return APR_MD5_FALSE;
    }
}

void Apr::rand_salt(char * const salt)
{
    int rv;
    char intSalt[SALT_LEN];
    char *b64Salt;
    int len = apu->apr_base64_encode_len(SALT_LEN);
    b64Salt = (char*) malloc(len * sizeof(char*));
    apu->apr_base64_encode(b64Salt, intSalt, SALT_LEN);
    rv = apu->apr_generate_random_bytes((unsigned char*) intSalt, SALT_LEN - 1);
    intSalt[SALT_LEN - 1] = '\0';
    apu->apr_base64_encode(b64Salt, intSalt, SALT_LEN);
    apu->apr_cpystrn(salt, b64Salt, SALT_LEN);
    free(b64Salt);
}

extern "C"
{
    DYNAPR_API Apr * newAprObject()
    {
        if (slock.lock())
        {
            if (!aprInt)
                aprInt = new Apr();
            slock.unlock();
        }
        return aprInt;
    }

    DYNAPR_API void destroyAprObject()
    {
        if (slock.lock())
        {
            delete aprInt;
            aprInt = NULL;
            slock.unlock();
        }
    }
}