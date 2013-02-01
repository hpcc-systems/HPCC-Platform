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

#ifndef APRSHARED_H
#define APRSHARED_H

#include "jlib.hpp"
#include "jstring.hpp"
#include "jutil.hpp"
#include "dynshared.hpp"

#define APR "apr-1"

typedef int (*apr_initialize_t)(void);
typedef int (*apr_terminate_t)(void);

class AprShared : public CInterface, public DynInit
{
public:
    IMPLEMENT_IINTERFACE;
    AprShared();
    void init();
    virtual ~AprShared()
    {
        delete so;
    }

    int apr_initialize();
    int apr_terminate();

private:
    SharedObject *so;
    HINSTANCE hndl;
    StringBuffer lib_name;
    Owned<SharedObjectFunction<apr_initialize_t> > _initialize;
    Owned<SharedObjectFunction<apr_terminate_t> > _terminate;

};

#endif