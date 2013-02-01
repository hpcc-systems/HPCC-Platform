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

#include "aprshared.hpp"
#include "jlib.hpp"
#include "jstring.hpp"
#include "jexcept.hpp"
#include "jutil.hpp"
#include "dynshared.hpp"

AprShared::AprShared()
{
    setName("AprShared");
    lib_name.append(SharedObjectPrefix).append(APR).append(SharedObjectExtension);
    so = new SharedObject();
    setInit(false);
}

void AprShared::init()
{
    if( !so->load(lib_name, true))
    {
        throw MakeStringException(-1, "load %s failed with code %d", lib_name.str(), GetSharedObjectError());
    }
    hndl = so->getInstanceHandle();
    _initialize.setown(new SharedObjectFunction<apr_initialize_t>(hndl, "apr_initialize"));
    _terminate.setown(new SharedObjectFunction<apr_terminate_t>(hndl, "apr_terminate"));
    setInit(true);
}
int AprShared::apr_initialize()
{
    checkInit();
    return (_initialize->getFuncPtr())();
}

int AprShared::apr_terminate()
{
    checkInit();
    return (_terminate->getFuncPtr())();
}
