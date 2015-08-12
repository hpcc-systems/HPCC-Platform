/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    Owned<IWorkUnit> wu = factory->updateWorkUnit(wuid, &secmgr, &secuser);
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
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    Owned<IWorkUnit> wu = factory->updateWorkUnit(wuid, &secmgr, &secuser);
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
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    factory->descheduleAllWorkUnits(&secmgr, &secuser);
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
