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
#include "jptree.hpp"
#include "jsocket.hpp"
#include "workunit.hpp"
#include "mpbase.hpp"
#include "dllserver.hpp"
#include "daclient.hpp"

#include "daclient.hpp"
#include "dasds.hpp"
#include "danqs.hpp"
#include "dalienv.hpp"

#ifdef _USE_CPPUNIT
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>
#endif

static unsigned testSize = 1000;

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
           "   unpack\n"
           "   validate [fix=1]\n");
}

bool dump(IConstWorkUnit &w, IProperties *globals)
{
    const char *action = globals->queryProp("#action");
    if (!action || stricmp(action, "list")==0)
    {
        Owned <IConstWUQuery> query = w.getQuery();
        SCMStringBuffer queryText;
        if (query)
            query->getQueryText(queryText);
        printf("%-20s %-10s %-10s %s\n", w.queryWuid(), w.queryJobName(), w.queryStateDesc(), queryText.str());
    }
    else if (stricmp(action, "results")==0)
    {
        Owned<IConstWUResultIterator> results = &w.getResults();
        ForEach(*results)
        {
            SCMStringBuffer xml;
            results->query().getResultXml(xml, true);
            printf("%s\n", xml.str());
            SCMStringBuffer schema;
            results->query().getResultEclSchema(schema);
            printf("%s\n", schema.str());
        }
    }
    else if (stricmp(action, "dump")==0)
    {
        SCMStringBuffer xml;
        exportWorkUnitToXML(&w, xml, true, false, true);
        printf("%s\n", xml.str());
    }
    else if (stricmp(action, "temporaries")==0)
    {
        Owned<IConstWUResult> r = w.getTemporaryByName("a8QL");
        printf("%s = %" I64F "d\n", "a8QL", r->getResultInt());
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
        StringAttr wuid(w.queryWuid());
        {
            MTIME_SECTION(queryActiveTimer(), "deleteWorkunit");
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
        StringAttr wuid(w.queryWuid());
        if (QUERYINTERFACE(&w, IExtendedWUInterface)->archiveWorkUnit(to.str(), globals->getPropBool("DEL", false), true, !globals->getPropBool("KEEPFILERESULTS", false)))
            printf("archived %s\n", wuid.str());
        else
            printf("archive of %s failed\n", wuid.str());
    }
    else if ((stricmp(action, "pack")==0)||(stricmp(action, "unpack")==0))
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        StringAttr wuid(w.queryWuid());
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
            IConstWorkUnitInfo& wu = it->query();
            printf("%d: %s, %s, %d\n", n, wu.queryWuid(), wu.queryUser(), (int)wu.getState());
        }
    }
}

#ifdef FORCE_WORKUNITS_TO_CASSANDRA
extern "C" IWorkUnitFactory *createWorkUnitFactory(const IPropertyTree *props);
#endif

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
#ifdef FORCE_WORKUNITS_TO_CASSANDRA
    StringBuffer cassandraServer;
    if (globals->getProp("CASSANDRASERVER", cassandraServer))
    {
        // Statically linking to cassandra plugin makes debugging easier (and means can debug simple cassandra workunit interactions without needing dali running)
        Owned<IPTree> props = createPTreeFromXMLString("<WorkUnitsServer><Option name='server' value='.'/><Option name='randomWuidSuffix' value='4'/><Option name='traceLevel' value='0'/></WorkUnitsServer>");
        props->setProp("Option[@name='server']/@value", cassandraServer.str());
        props->setPropInt("Option[@name='traceLevel']/@value", globals->getPropInt("tracelevel", 0));
        setWorkUnitFactory(createWorkUnitFactory(props));
    }
#endif

    StringBuffer daliServers;
    if (!globals->getProp("DALISERVERS", daliServers))
        daliServers.append(".");
    if (!strieq(daliServers, "none"))
    {
        Owned<IGroup> serverGroup = createIGroup(daliServers.str(), DALI_SERVER_PORT);
        initClientProcess(serverGroup,DCR_Other);
        setPasswordsFromSDS();
    }
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
        if (action && (stricmp(action, "testpaged")==0))
        {
            testPagedWuList(factory);
        }
#ifdef _USE_CPPUNIT
        else if (action && (stricmp(action, "-selftest")==0))
        {
            testSize = globals->getPropInt("testSize", 1000);
            queryStderrLogMsgHandler()->setMessageFields(MSGFIELD_time | MSGFIELD_milliTime | MSGFIELD_prefix);
            CppUnit::TextUi::TestRunner runner;
            CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry("WuTest");
            runner.addTest( registry.makeTest() );
            bool wasSucessful = runner.run( "", false );
            return wasSucessful;
        }
#endif
        else if (action && (stricmp(action, "validate")==0))
        {
            bool fix = globals->getPropBool("fix", false);
            unsigned errors = factory->validateRepository(fix);
            printf("%u errors %s\n", errors, (fix && errors) ? "fixed" : "found");
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
                        IConstWorkUnitInfo& w = it->query();
                        if (!w.isProtected() || globals->getPropBool("protected", false))
                        {
                            const char *wuid = w.queryWuid();
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
                                        Owned<IWorkUnit> lw = factory->updateWorkUnit(wuid);
                                        lw->protect(false);
                                        lw->setState(WUStateArchived);  // we are killing anyway so it's irrelevant, but this will allow us to be sure we can kill it...
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
        }
        else if (globals->hasProp("WUID"))
        {
            if (action && stricmp(action, "restore")==0)
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
                IConstWorkUnitInfo& wi = it->query();
                Owned<IConstWorkUnit> w = factory->openWorkUnit(wi.queryWuid(), false);
                if (!dump(*w, globals))
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
    if (queryActiveTimer())
    {
        queryActiveTimer()->printTimings();
        queryActiveTimer()->reset();
    }
    releaseAtoms();
    return 0;
}

#ifdef _USE_CPPUNIT
#include "unittests.hpp"

class WuTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(WuTest);
        CPPUNIT_TEST(testCreate);
        CPPUNIT_TEST(testList);
        CPPUNIT_TEST(testDelete);
    CPPUNIT_TEST_SUITE_END();
protected:
    static StringArray wuids;
    void testCreate()
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        unsigned before = factory->numWorkUnits();
        unsigned start = msTick();
        for (int i = 0; i < testSize; i++)
        {
            VStringBuffer userId("WuTestUser%d", i % 10);
            Owned<IWorkUnit>wu = factory->createWorkUnit("WuTest", NULL, NULL, NULL);
            wu->setState(WUStateFailed);
            wu->setUser(userId);
            wuids.append(wu->queryWuid());
        }
        unsigned after = factory->numWorkUnits();
        DBGLOG("%u workunits created in %d ms (%d total)", testSize, msTick()-start, after);
        ASSERT(after-before==testSize);
        ASSERT(wuids.length() == testSize);
    }

    void testDelete()
    {
        ASSERT(wuids.length() == testSize);
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        unsigned before = factory->numWorkUnits();
        unsigned start = msTick();
        for (int i = 0; i < testSize; i++)
        {
            factory->deleteWorkUnit(wuids.item(i));
        }
        unsigned after = factory->numWorkUnits();
        DBGLOG("%u workunits deleted in %d ms (%d remain)", testSize, msTick()-start, after);
        ASSERT(before-after==testSize);
    }

    void testList()
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        unsigned before = factory->numWorkUnits();
        unsigned start = msTick();
        unsigned numIterated = 0;
        Owned<IConstWorkUnitIterator> wus = factory->getWorkUnitsByOwner(NULL, NULL, NULL);
        ForEach(*wus)
        {
            IConstWorkUnitInfo &wu = wus->query();
            numIterated++;
        }
        DBGLOG("%d workunits listed in %d ms", numIterated, msTick()-start);
        ASSERT(numIterated == before);
        // Now by owner
        wus.setown(factory->getWorkUnitsByOwner("WuTestUser0", NULL, NULL));
        start = msTick();
        numIterated = 0;
        ForEach(*wus)
        {
            IConstWorkUnitInfo &wu = wus->query();
            ASSERT(streq(wu.queryUser(), "WuTestUser0"));
            numIterated++;
        }
        DBGLOG("%d workunits listed in %d ms", numIterated, msTick()-start);
        ASSERT(numIterated == (testSize+9)/10);
    }
};
StringArray WuTest::wuids;

CPPUNIT_TEST_SUITE_REGISTRATION( WuTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( WuTest, "WuTest" );

#endif
