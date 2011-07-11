/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

