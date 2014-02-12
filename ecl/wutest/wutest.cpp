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
#include "jlib.hpp"
#include "jfile.hpp"
#include "jprop.hpp"
#include "jsocket.hpp"
#include "workunit.hpp"
#include "mpbase.hpp"
#include "dllserver.hpp"
#include "daclient.hpp"

#include "daclient.hpp"
#include "dasds.hpp"
#include "danqs.hpp"
#include "dalienv.hpp"

void usage()
{
    printf("Usage: WUTEST action [WUID=xxx] [OWNER=xxx]\n\n"
           "actions supported are:\n"
           "   list\n"
           "   dump\n"
           "   delete\n"
           "   results\n"
           "   archive [TO=<directory>] [DEL=1] [KEEPFILERESULTS=1]\n"
           "   restore [FROM=<directory>]\n"
           "   pack\n"
           "   unpack\n");
}

bool dump(IConstWorkUnit &w, IProperties *globals)
{
    const char *action = globals->queryProp("#action");
    if (!action || stricmp(action, "list")==0)
    {
        SCMStringBuffer wuid, jobname, state, parent;
        w.getWuid(wuid);
        w.getParentWuid(parent);
        w.getStateDesc(state);
        w.getJobName(jobname);
        printf("%-30s %-20s %-10s %-30s\n", wuid.str(), jobname.str(), state.str(), parent.str());
    }
    else if (stricmp(action, "results")==0)
    {
        Owned<IConstWUResultIterator> results = &w.getResults();
        ForEach(*results)
        {
            SCMStringBuffer xml;
            results->query().getResultXml(xml);
            printf("%s\n", xml.str());
            SCMStringBuffer schema;
            results->query().getResultEclSchema(schema);
            printf("%s\n", schema.str());
        }
    }
    else if (stricmp(action, "dump")==0)
    {
        SCMStringBuffer xml;
        exportWorkUnitToXML(&w, xml, true, false);
        printf("%s\n", xml.str());
    }
    else if (stricmp(action, "temporaries")==0)
    {
        Owned<IConstWUResult> r = w.getTemporaryByName("a8QL");
        printf("%s = %"I64F"d\n", "a8QL", r->getResultInt());
    }
    else if (stricmp(action, "get")==0)
    {
        Owned<IConstWUQuery> q = w.getQuery();
        SCMStringBuffer x;
        q->getQueryText(x);
        printf("%s\n", x.str());
    }
    else if (stricmp(action, "delete")==0)
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        SCMStringBuffer wuid, jobname;
        {
            MTIME_SECTION(timer, "getWUID");
            w.getWuid(wuid);
        }
        {
            MTIME_SECTION(timer, "deleteWorkunit");
            factory->deleteWorkUnit(wuid.str());
        }
        printf("deleted %s\n", wuid.str());
    }
    else if (stricmp(action, "archive")==0)
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        StringBuffer to;
        globals->getProp("TO", to);
        if (to.length()==0)
            to.append('.');
        SCMStringBuffer wuid;
        w.getWuid(wuid);
        if (QUERYINTERFACE(&w, IExtendedWUInterface)->archiveWorkUnit(to.str(),globals->getPropBool("DEL",false),true,!globals->getPropBool("KEEPFILERESULTS", false)))
            printf("archived %s\n", wuid.str());
        else
            printf("archive of %s failed\n", wuid.str());
    }
    else if ((stricmp(action, "pack")==0)||(stricmp(action, "unpack")==0))
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        SCMStringBuffer wuid;
        w.getWuid(wuid);
        bool pack = (stricmp(action, "pack")==0);
        QUERYINTERFACE(&w, IExtendedWUInterface)->packWorkUnit(pack);
        if (pack)
            printf("packed %s \n", wuid.str());
        else
            printf("unpacked %s\n", wuid.str());
    }
    else if (stricmp(action, "getWebServicesInfo")==0)
    {
        Owned<IConstWUWebServicesInfo> q = w.getWebServicesInfo();
        SCMStringBuffer x;
        q->getInfo(NULL, x);
        printf("%s\n", x.str());
    }

    else {
        usage();
        return false;
    }
    return true;
}

void testPagedWuList(IWorkUnitFactory *factory)
{
    __int64 cachehint=0;
    unsigned n=0;
    for (unsigned page=0;page<3;page++) {
        WUSortField sortorder[] = {WUSFuser,WUSFstate,WUSFterm};
        Owned<IConstWorkUnitIterator> it = factory->getWorkUnitsSorted(sortorder, NULL, NULL, page*10, 10, "nigel", &cachehint, NULL);
        ForEach(*it) {
            n++;
            IConstWorkUnit& wu = it->query();
            SCMStringBuffer wuid;
            wu.getWuid(wuid);
            SCMStringBuffer user;
            wu.getUser(user);
            printf("%d: %s, %s, %d\n",n,wuid.str(),user.str(),(int)wu.getState());
        }
    }
}


int main(int argc, const char *argv[])
{
    InitModuleObjects();
    unsigned count=0;
    Owned<IProperties> globals = createProperties("WUTEST.INI", true);
    for (int i = 1; i < argc; i++)
    {
        if (strchr(argv[i],'='))
            globals->loadProp(argv[i]);
        else if (argv[i][0]=='W' || argv[i][0]=='w')
            globals->setProp("WUID", argv[i]);
        else
            globals->setProp("#action", argv[i]);
    }

    StringBuffer daliServers;
    if (!globals->getProp("DALISERVERS", daliServers))
        daliServers.append(".");
    Owned<IGroup> serverGroup = createIGroup(daliServers.str(), DALI_SERVER_PORT);
    initClientProcess(serverGroup,DCR_Other);
    setPasswordsFromSDS();
    try
    {
        CDateTime cutoff;
        int days = globals->getPropInt("days", 0);
        if (days)
        {
            cutoff.setNow();
            cutoff.adjustTime(-60*24*days);
        }
        else
        {
            const char *since = globals->queryProp("since");
            if (since)
                cutoff.setDateString(since, NULL);
        }
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        const char *action = globals->queryProp("#action");
        if (action && (stricmp(action, "testpaged")==0)) {
            testPagedWuList(factory);
        }
        else if (action && (stricmp(action, "orphans")==0 || stricmp(action, "cleanup")==0))
        {
            factory->setTracingLevel(0);
            StringArray killWuids;
            if (stricmp(action, "cleanup")==0)
            {
                // Delete unprotected workunits older than nn days
                if (!cutoff.isNull())
                {
                    Owned<IConstWorkUnitIterator> it = factory->getWorkUnitsByOwner(globals->queryProp("OWNER"));
                    ForEach(*it)
                    {
                        IConstWorkUnit& w = it->query();
                        if (!w.isProtected() || globals->getPropBool("protected", false))
                        {
                            SCMStringBuffer wuidstr;
                            w.getWuid(wuidstr);
                            const char *wuid = wuidstr.str();
                            unsigned year,month,day,hour,min,sec;
                            if (sscanf(wuid, "W%4u%2u%2u-%2u%2u%2u", &year, &month, &day, &hour, &min, &sec)==6)
                            {
                                CDateTime wuTime;
                                wuTime.setDate(year, month, day);
                                wuTime.setTime(hour, min, sec, 0, true);
                                if (wuTime.compare(cutoff) < 0)
                                {
                                    printf("Aged workunit %s\n", wuid);
                                    if (globals->getPropInt("remove", 0))
                                    {
                                        Owned<IWorkUnit> lw = &w.lock();
                                        lw->protect(false);
                                        lw->setState(WUStateArchived);  // we are killing anyway so it's irrelevent, but this will allow us to be sure we can kill it...
                                        lw.clear();
                                        killWuids.append(wuid);
                                    }
                                }
                            }
                        }
                    }
                }
                ForEachItemIn(idx, killWuids)
                {
                    factory->deleteWorkUnit(killWuids.item(idx));
                }
                killWuids.kill();

                // Delete CachedWorkunits
                Owned<IRemoteConnection> conn = querySDS().connect("/CachedWorkUnits", myProcessSession(), 0, 30000);
                if (conn)
                {
                    Owned<IPropertyTree> root = conn->getRoot();
                    Owned<IPropertyTreeIterator> iter = root->getElements("*/*");
                    unsigned i = 0;
                    ForEach(*iter)
                        i++;
                    printf("%d cached workunits\n", i);
                    if (globals->getPropInt("remove", 0))
                        conn->close(true);
                }

                // Trickier: delete plugins from GeneratedDlls that are not currently deployed anywhere
            }
            // After cleanup check for orphans....

            Owned<IRemoteConnection> conn = querySDS().connect("/GraphProgress", myProcessSession(), 0, 30000);
            if (conn)
            {
                Owned<IPropertyTree> root = conn->getRoot();
                Owned<IPropertyTreeIterator> iter = root->getElements("*");
                ForEach(*iter)
                {
                    IPropertyTree &thisProgress = iter->query();
                    const char *wuid = thisProgress.queryName();
                    Owned<IConstWorkUnit> w = factory->openWorkUnit(wuid, false);
                    if (!w)
                    {
                        printf("Orphaned graph info %s\n", wuid);
                        if (globals->getPropInt("remove", 0))
                            root->removeProp(wuid);
                    }
                }
            }
#if 0
            // dll cloning makes this not safe.
            conn.setown(querySDS().connect("/GeneratedDlls", myProcessSession(), 0, 30000));
            if (conn)
            {
                Owned<IPropertyTree> root = conn->getRoot();
                Owned<IPropertyTreeIterator> iter = root->getElements("*");
                IArrayOf<IPropertyTree> goers;
                ForEach(*iter)
                {
                    IPropertyTree &thisDllinfo = iter->query();
                    const char *filename = thisDllinfo.queryProp("@name");
                    if (filename)
                    {
                        StringBuffer wuid;
                        splitFilename(filename, NULL, NULL, &wuid, NULL);
                        if (wuid.charAt(0)=='W' && wuid.charAt(1)=='2' && wuid.charAt(2)=='0') // Beware the Y2100 bug...
                        {
                            Owned<IConstWorkUnit> w = factory->openWorkUnit(wuid.str(), false);
                            if (!w)
                            {
                                printf("Orphaned dll info %s\n", wuid.str());
                                goers.append(*LINK(&thisDllinfo));
                            }
                        }
                    }
                }
                if (globals->getPropInt("remove", 0))
                {
                    ForEachItemIn(idx, goers)
                    {
                        IPropertyTree &thisDllinfo = goers.item(idx);
                        queryDllServer().removeDll(thisDllinfo.queryProp("@name"), thisDllinfo.getPropInt("@crc", 0), true);
                    }
                }
            }
#endif
            Owned<IConstWorkUnitIterator> it = factory->getWorkUnitsByOwner(globals->queryProp("OWNER"));
            ForEach(*it)
            {
                IConstWorkUnit& w = it->query();
                SCMStringBuffer pw;
                w.getParentWuid(pw);
                if (pw.length())
                {
                    Owned<IConstWorkUnit> p = factory->openWorkUnit(pw.str(), false);
                    if (!p)
                    {
                        SCMStringBuffer wuidstr;
                        w.getWuid(wuidstr);
                        const char *wuid = wuidstr.str();
                        printf("Orphaned child workunit %s\n", wuid);
                        if (globals->getPropInt("remove", 0))
                        {
                            Owned<IWorkUnit> lw = &w.lock();
                            lw->setState(WUStateArchived);  // we are killing anyway so it's irrelevent, but this will allow us to be sure we can kill it...
                            lw->protect(false);
                            lw.clear();
                            killWuids.append(wuid);
                        }
                    }
                }
                ForEachItemIn(idx, killWuids)
                {
                    factory->deleteWorkUnit(killWuids.item(idx));
                }
                killWuids.kill();
            }
        }
        else if (globals->hasProp("WUID"))
        {
            if (stricmp(globals->queryProp("#action"), "restore")==0)
            {
                StringBuffer from;
                globals->getProp("FROM", from);
                if (from.length()==0)
                    from.append('.');
                const char *wuid = globals->queryProp("WUID");
                if (restoreWorkUnit(from.str(),wuid))
                    printf("restored %s\n", wuid);
                else
                    printf("failed to restore %s\n", wuid);
            }
            else {
                Owned<IConstWorkUnit> w = factory->openWorkUnit(globals->queryProp("WUID"), false);
                if (w)
                    dump(*w, globals);
            }
        }
        else 
        {
            Owned<IConstWorkUnitIterator> it = factory->getWorkUnitsByOwner(globals->queryProp("OWNER"));
            
            ForEach(*it)
            {
                IConstWorkUnit& w = it->query();
                if (!dump(w, globals))
                    break;
            }
            
        }
    }
    catch (IException *E)
    {
        StringBuffer m;
        printf("Error: %s\n", E->errorMessage(m).str());
        E->Release();
    }
    closeDllServer();   
    closeEnvironment(); 
    closedownClientProcess();   // dali client closedown
    if (timer)
    {
        timer->printTimings();
        timer->reset();
        timer = NULL;
    }
    releaseAtoms();
    return 0;
}

