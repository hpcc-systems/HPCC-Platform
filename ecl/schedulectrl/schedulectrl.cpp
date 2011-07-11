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
#include "jutil.hpp"
#include "jptree.hpp"
#include "jstring.hpp"
#include "dasds.hpp"
#include "schedulectrl.hpp"
#include "schedulectrl.ipp"

void scheduleWorkUnit(char const * wuid)
{
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    Owned<IWorkUnit> wu = factory->updateWorkUnit(wuid);
    if(wu)
        wu->schedule();
}

void scheduleWorkUnit(char const * wuid, ISecManager & secmgr, ISecUser & secuser)
{
    Owned<IWorkUnitFactory> factory = getSecWorkUnitFactory(secmgr, secuser);
    Owned<IWorkUnit> wu = factory->updateWorkUnit(wuid);
    if(wu)
        wu->schedule();
}

void descheduleWorkUnit(char const * wuid)
{
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    Owned<IWorkUnit> wu = factory->updateWorkUnit(wuid);
    if(wu)
        wu->deschedule();
}

void descheduleWorkUnit(char const * wuid, ISecManager & secmgr, ISecUser & secuser)
{
    Owned<IWorkUnitFactory> factory = getSecWorkUnitFactory(secmgr, secuser);
    Owned<IWorkUnit> wu = factory->updateWorkUnit(wuid);
    if(wu)
        wu->deschedule();

}

void descheduleAllWorkUnits()
{
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    factory->descheduleAllWorkUnits();
}

void descheduleAllWorkUnits(ISecManager & secmgr, ISecUser & secuser)
{
    Owned<IWorkUnitFactory> factory = getSecWorkUnitFactory(secmgr, secuser);
    factory->descheduleAllWorkUnits();
}

void descheduleNonexistentWorkUnit(char const * wuid)
{
    StringBuffer xpath;
    xpath.append("*/*/*/");
    ncnameEscape(wuid, xpath);
    Owned<IRemoteConnection> conn = querySDS().connect("/Schedule", myProcessSession(), RTM_LOCK_WRITE, connectionTimeout);
    if(!conn) return;
    PROGLOG("Scheduled workunit %s could not be found, and so is being descheduled", wuid);
    Owned<IPropertyTree> root = conn->getRoot();
    bool more;
    do more = root->removeProp(xpath.str()); while(more);
}

bool isScheduledWorkUnit(char const * wuid)
{
    Owned<IRemoteConnection> conn = querySDS().connect("/Schedule", myProcessSession(), RTM_LOCK_WRITE | RTM_CREATE_QUERY, connectionTimeout);
    StringBuffer xpath("*/*/*/");
    ncnameEscape(wuid, xpath);
    return conn->queryRoot()->hasProp(xpath.str());
}

void recursiveCleanup(IPropertyTree * tree, unsigned level)
{
    Owned<IPropertyTreeIterator> iter = tree->getElements("*");
    for(iter->first(); iter->isValid(); iter->next())
    {
        if(level)
            recursiveCleanup(&iter->query(), level-1);
        if(!iter->query().hasChildren())
            iter->query().setProp("@remove", "yes");
    }
    bool more;
    do more = tree->removeProp("*[@remove=\"yes\"]"); while(more);
}

void cleanupSchedulerList(IPropertyTree * schedule)
{
    Owned<IRemoteConnection> conn = querySDS().connect("/Schedulers", myProcessSession(), RTM_LOCK_WRITE, connectionTimeout);
    if(!conn) return;
    Owned<IPropertyTree> root(conn->queryRoot()->getBranch("."));
    Owned<IPropertyTreeIterator> iter = root->getElements("*");
    for(iter->first(); iter->isValid(); iter->next())
        if(!schedule->hasProp(iter->query().queryName()))
            iter->query().setProp("@remove", "yes");
    bool more;
    do more = root->removeProp("*[@remove=\"yes\"]"); while(more);
}

void cleanupWorkUnitSchedule()
{
    Owned<IRemoteConnection> conn = querySDS().connect("/Schedule", myProcessSession(), RTM_LOCK_WRITE, connectionTimeout);
    if(!conn) return;
    Owned<IPropertyTree> root(conn->queryRoot()->getBranch("."));
    recursiveCleanup(root, 2);
    conn->commit();
    cleanupSchedulerList(root);
}
