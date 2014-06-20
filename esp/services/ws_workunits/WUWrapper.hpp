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

// WorkUnit.h: interface for the CWorkUnit class.
//
//////////////////////////////////////////////////////////////////////
#ifndef _ESPWIZ_CWUWrapper_HPP__
#define _ESPWIZ_CWUWrapper_HPP__

#include "jiface.hpp"
#include "jexcept.hpp"
#include "workunit.hpp"
#include "esp.hpp"
#include "exception_util.hpp"

class CWUWrapper : public CInterface
{
public:
    CWUWrapper() {}
    CWUWrapper(const char* wuid, IEspContext &context): 
        factory(context.querySecManager() ? getSecWorkUnitFactory(*context.querySecManager(), *context.queryUser()) : getWorkUnitFactory()), 
        wu(factory->openWorkUnit(wuid, false))
    {
        if(!wu)
            throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Could not open workunit %s",wuid);
    }

    CWUWrapper(const char* wuid): factory(getWorkUnitFactory()), wu(factory->openWorkUnit(wuid, false))
    {
        if(!wu)
            throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Could not open workunit %s",wuid);
    }

    CWUWrapper(const char * parentWuid, const char * app, const char * user, IEspContext &context): 
        factory(getSecWorkUnitFactory(*context.querySecManager(), *context.queryUser())), wu(factory->createWorkUnit(parentWuid, app, user))
    {
        if(!wu)
            throw MakeStringException(ECLWATCH_CANNOT_CREATE_WORKUNIT,"Could not create workunit.");
    }

    operator IConstWorkUnit* () { return wu.get(); }
    IConstWorkUnit* operator->() { return wu.get(); }
    
    void clear()
    {
        wu.clear();
        factory.clear();
    }

    bool isRunning()
    {
        // MORE - move into workunit interface
        switch (wu->getState())
        {
        case WUStateFailed:
        case WUStateAborted:
        case WUStateCompleted:
            return false;
        default:
            return true;
        }
    }

protected:
    Owned<IWorkUnitFactory> factory;
    Owned<IConstWorkUnit> wu;
};

#endif // _ESPWIZ_CWUWrapper_HPP__

