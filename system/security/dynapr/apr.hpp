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

#ifndef APR_H
#define APR_H

#include "jlib.hpp"
#include "jmutex.hpp"
#include "jstring.hpp"
#include "dynshared.hpp"
#include "aprshared.hpp"
#include "apushared.hpp"

#ifndef DYNAPR_API
#    ifdef _WIN32
#        ifndef DYNAPR_EXPORTS
#            define DYNAPR_API __declspec(dllimport)
#        else
#            define DYNAPR_API __declspec(dllexport)
#        endif
#    else
#        define DYNAPR_API
#    endif
#endif


enum apr_enum {
    APR_MD5_NULL=-1,
    APR_MD5_FALSE=0,
    APR_MD5_TRUE=1
};

typedef apr_enum apr_ret_t;

class DYNAPR_API Apr : public CInterface, public DynInit
{
public:
    IMPLEMENT_IINTERFACE;
    Apr();
    virtual ~Apr();
    void init();
    void apr_md5_string(StringBuffer& inpstring, StringBuffer& outstring);
    apr_ret_t apr_md5_validate(StringBuffer& inpstring, StringBuffer& aprmd5string);

private:
    Owned<AprShared> apr;
    Owned<ApuShared> apu;

    void rand_salt(char * const salt);

};

static Apr *aprInt = NULL;
static CSingletonLock slock;

extern "C" DYNAPR_API Apr * newAprObject();
extern "C" DYNAPR_API void destroyAprObject();

#endif
