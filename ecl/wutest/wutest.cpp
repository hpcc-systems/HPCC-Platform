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
#include "dautils.hpp"
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
        StringBuffer xml;
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
        Owned<IPTree> props = createPTreeFromXMLString("<WorkUnitsServer><Option name='server' value='.'/><Option name='randomWuidSuffix' value='4'/><Option name='traceLevel' value='0'/><Option name='keyspace' value='hpcc_test'></Option></WorkUnitsServer>");
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
#ifdef _USE_CPPUNIT
        if (action && (stricmp(action, "-selftest")==0))
        {
            testSize = globals->getPropInt("testSize", 100);
            queryStderrLogMsgHandler()->setMessageFields(MSGFIELD_time | MSGFIELD_milliTime | MSGFIELD_prefix);
            CppUnit::TextUi::TestRunner runner;
            CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry("WuTest");
            runner.addTest( registry.makeTest() );
            bool wasSucessful = runner.run( "", false );
            return wasSucessful;
        }
        else
#endif
        if (action && (stricmp(action, "validate")==0))
        {
            bool fix = globals->getPropBool("fix", false);
            unsigned errors = factory->validateRepository(fix);
            printf("%u errors %s\n", errors, (fix && errors) ? "fixed" : "found");
        }
        else if (action && (stricmp(action, "clear")==0))
        {
            if (globals->getPropBool("entire", false) && globals->getPropBool("repository", false))
            {
                factory->deleteRepository(false);
                printf("Repository deleted\n");
            }
            else
                printf("You need to specify entire=1 and repository=1 to delete entire repository\n");
        }
        else if (action && (stricmp(action, "initialize")==0))
        {
            factory->createRepository();
            printf("Repository created\n");
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
        CPPUNIT_TEST(testSet);
        CPPUNIT_TEST(testDelete);
        CPPUNIT_TEST(testCopy);
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
            VStringBuffer userId("WuTestUser%d", i % 50);
            VStringBuffer clusterName("WuTestCluster%d", i % 5);
            VStringBuffer jobName("WuTest job %d", i % 3);
            Owned<IWorkUnit>wu = factory->createWorkUnit("WuTest", NULL, NULL, NULL);
            if (i % 6)
                wu->setState(WUStateCompleted);
            else
                wu->setState(WUStateFailed);
            wu->setUser(userId);
            wu->setClusterName(clusterName);
            if (i % 3)
                wu->setJobName(jobName);
            wuids.append(wu->queryWuid());
        }
        unsigned after = factory->numWorkUnits();
        DBGLOG("%u workunits created in %d ms (%d total)", testSize, msTick()-start, after);
        ASSERT(after-before==testSize);
        ASSERT(wuids.length() == testSize);
        start = msTick();
        ASSERT(factory->validateRepository(false)==0);
        DBGLOG("%u workunits validated in %d ms", after, msTick()-start);
    }

    void testCopy()
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        Owned<IWorkUnit> createWu = factory->createWorkUnit("WuTest", NULL, NULL, NULL);
        StringBuffer wuid(createWu->queryWuid());
        Owned<ILocalWorkUnit> embeddedWU = createLocalWorkUnit(
                // Note - generated by compiling the following ecl:
                //
                //   integer one := 1 : stored('one');
                //   d := nofold(dataset([{1}], { integer v}));
                //   ones := count(d(v=one)) : independent;
                //   d(v = ones);
                //
                // then running
                //
                //   ./a.out DUMPFINALWU=1 | sed "s/\"/'/g" | sed s/^/\"/ | sed s/$/\"/
                //
                // then a little trimming to remove some of the statistics, sort the wfid's on the workflow, and add sequence=-1 on the variable.
                "<W_LOCAL buildVersion='community_6.0.0-trunk0Debug[heads/cass-wu-part3-0-g10b954-dirty]'"
                "         cloneable='1'"
                "         clusterName=''"
                "         codeVersion='158'"
                "         eclVersion='6.0.0'"
                "         hash='2796091347'"
                "         state='completed'"
                "         xmlns:xsi='http://www.w3.org/1999/XMLSchema-instance'>"
                " <Debug>"
                "  <debugquery>1</debugquery>"
                "  <expandpersistinputdependencies>1</expandpersistinputdependencies>"
                "  <savecpptempfiles>1</savecpptempfiles>"
                "  <saveecltempfiles>1</saveecltempfiles>"
                "  <spanmultiplecpp>0</spanmultiplecpp>"
                "  <standaloneexe>1</standaloneexe>"
                "  <targetclustertype>hthor</targetclustertype>"
                " </Debug>"
                " <Graphs>"
                "  <Graph name='graph1' type='activities'>"
                "   <xgmml>"
                "    <graph wfid='2'>"
                "     <node id='1'>"
                "      <att>"
                "       <graph>"
                "        <att name='rootGraph' value='1'/>"
                "        <edge id='2_0' source='2' target='3'/>"
                "        <edge id='3_0' source='3' target='4'/>"
                "        <edge id='4_0' source='4' target='5'/>"
                "        <node id='2' label='Inline Row&#10;{1}'>"
                "         <att name='definition' value='./sets.ecl(2,13)'/>"
                "         <att name='_kind' value='148'/>"
                "         <att name='ecl' value='ROW(TRANSFORM({ integer8 v },SELF.v := 1;));&#10;'/>"
                "         <att name='recordSize' value='8'/>"
                "         <att name='predictedCount' value='1'/>"
                "        </node>"
                "        <node id='3' label='Filter'>"
                "         <att name='definition' value='./sets.ecl(3,15)'/>"
                "         <att name='_kind' value='5'/>"
                "         <att name='ecl' value='FILTER(v = STORED(&apos;one&apos;));&#10;'/>"
                "         <att name='recordSize' value='8'/>"
                "         <att name='predictedCount' value='0..?[disk]'/>"
                "        </node>"
                "        <node id='4' label='Count'>"
                "         <att name='_kind' value='125'/>"
                "         <att name='ecl' value='TABLE({ integer8 value := COUNT(group) });&#10;'/>"
                "         <att name='recordSize' value='8'/>"
                "         <att name='predictedCount' value='1'/>"
                "        </node>"
                "        <node id='5' label='Store&#10;Internal(&apos;wf2&apos;)'>"
                "         <att name='_kind' value='22'/>"
                "         <att name='ecl' value='extractresult(value, named(&apos;wf2&apos;));&#10;'/>"
                "         <att name='recordSize' value='8'/>"
                "        </node>"
                "       </graph>"
                "      </att>"
                "     </node>"
                "    </graph>"
                "   </xgmml>"
                "  </Graph>"
                "  <Graph name='graph2' type='activities'>"
                "   <xgmml>"
                "    <graph wfid='3'>"
                "     <node id='6'>"
                "      <att>"
                "       <graph>"
                "        <att name='rootGraph' value='1'/>"
                "        <edge id='7_0' source='7' target='8'/>"
                "        <edge id='8_0' source='8' target='9'/>"
                "        <node id='7' label='Inline Row&#10;{1}'>"
                "         <att name='definition' value='./sets.ecl(2,13)'/>"
                "         <att name='_kind' value='148'/>"
                "         <att name='ecl' value='ROW(TRANSFORM({ integer8 v },SELF.v := 1;));&#10;'/>"
                "         <att name='recordSize' value='8'/>"
                "         <att name='predictedCount' value='1'/>"
                "        </node>"
                "        <node id='8' label='Filter'>"
                "         <att name='definition' value='./sets.ecl(5,1)'/>"
                "         <att name='_kind' value='5'/>"
                "         <att name='ecl' value='FILTER(v = INTERNAL(&apos;wf2&apos;));&#10;'/>"
                "         <att name='recordSize' value='8'/>"
                "         <att name='predictedCount' value='0..?[disk]'/>"
                "        </node>"
                "        <node id='9' label='Output&#10;Result #1'>"
                "         <att name='definition' value='./sets.ecl(1,1)'/>"
                "         <att name='name' value='sets'/>"
                "         <att name='definition' value='./sets.ecl(5,1)'/>"
                "         <att name='_kind' value='16'/>"
                "         <att name='ecl' value='OUTPUT(..., workunit);&#10;'/>"
                "         <att name='recordSize' value='8'/>"
                "        </node>"
                "       </graph>"
                "      </att>"
                "     </node>"
                "    </graph>"
                "   </xgmml>"
                "  </Graph>"
                " </Graphs>"
                " <Query fetchEntire='1'>"
                "  <Associated>"
                "   <File desc='a.out.cpp'"
                "         filename='/Users/rchapman/HPCC-Platform/ossd/a.out.cpp'"
                "         ip='192.168.2.203'"
                "         type='cpp'/>"
                "  </Associated>"
                " </Query>"
                " <Results>"
                "  <Result isScalar='0'"
                "          name='Result 1'"
                "          recordSizeEntry='mf1'"
                "          rowLimit='-1'"
                "          sequence='0'"
                "          status='calculated'>"
                "   <rowCount>1</rowCount>"
                "   <SchemaRaw xsi:type='SOAP-ENC:base64'>"
                "    dgABCAEAGBAAAAB7IGludGVnZXI4IHYgfTsK   </SchemaRaw>"
                "   <totalRowCount>1</totalRowCount>"
                "   <Value xsi:type='SOAP-ENC:base64'>"
                "    AQAAAAAAAAA=   </Value>"
                "  </Result>"
                " </Results>"
                " <Statistics>"
                "  <Statistic c='eclcc'"
                "             count='1'"
                "             creator='eclcc'"
                "             kind='TimeElapsed'"
                "             s='compile'"
                "             scope='compile:parseTime'"
                "             ts='1431603789722535'"
                "             unit='ns'"
                "             value='805622'/>"
                "  <Statistic c='unknown'"
                "             count='1'"
                "             creator='unknownRichards-iMac.local'"
                "             kind='WhenQueryStarted'"
                "             s='global'"
                "             scope='workunit'"
                "             ts='1431603790007020'"
                "             unit='ts'"
                "             value='1431603790007001'/>"
                "  <Statistic c='unknown'"
                "             count='1'"
                "             creator='unknownRichards-iMac.local'"
                "             desc='Graph graph1'"
                "             kind='TimeElapsed'"
                "             s='graph'"
                "             scope='graph1'"
                "             ts='1431603790007912'"
                "             unit='ns'"
                "             value='0'/>"
                " </Statistics>"
                " <Temporaries>"
                "  <Variable name='wf2' status='calculated'>"
                "   <rowCount>1</rowCount>"
                "   <totalRowCount>1</totalRowCount>"
                "   <Value xsi:type='SOAP-ENC:base64'>"
                "    AQAAAAAAAAA=   </Value>"
                "  </Variable>"
                " </Temporaries>"
                " <Tracing>"
                "  <EclAgentBuild>community_6.0.0-trunk0Debug[heads/cass-wu-part3-0-g10b954-dirty]</EclAgentBuild>"
                " </Tracing>"
                " <Variables>"
                "  <Variable name='one' sequence='-1' status='calculated'>"
                "   <rowCount>1</rowCount>"
                "   <SchemaRaw xsi:type='SOAP-ENC:base64'>"
                "    b25lAAEIAQAYAAAAAA==   </SchemaRaw>"
                "   <totalRowCount>1</totalRowCount>"
                "   <Value xsi:type='SOAP-ENC:base64'>"
                "    AQAAAAAAAAA=   </Value>"
                "  </Variable>"
                " </Variables>"
                " <Workflow>"
                "  <Item mode='normal'"
                "        state='done'"
                "        type='normal'"
                "        wfid='1'/>"
                "  <Item mode='normal'"
                "        state='done'"
                "        type='normal'"
                "        wfid='2'>"
                "   <Dependency wfid='1'/>"
                "  </Item>"
                "  <Item mode='normal'"
                "        state='done'"
                "        type='normal'"
                "        wfid='3'>"
                "   <Dependency wfid='2'/>"
                "   <Schedule/>"
                "  </Item>"
                " </Workflow>"
                "</W_LOCAL>"
                );
        StringBuffer xml1, xml2, xml3;
        exportWorkUnitToXML(embeddedWU, xml1, false, false, false);
        queryExtendedWU(createWu)->copyWorkUnit(embeddedWU, true);
        createWu->setState(WUStateCompleted);
        exportWorkUnitToXML(createWu, xml2, false, false, false);
        createWu->commit();
        createWu.clear();

        // Now try to re-read
        Owned<IConstWorkUnit> wu = factory->openWorkUnit(wuid, false);
        ASSERT(streq(wu->queryWuid(), wuid));
        ASSERT(streq(wu->queryJobName(), embeddedWU->queryJobName()));
        exportWorkUnitToXML(wu, xml3, false, false, false);
        // Check that writing to/reading from the server leaves unmodified
        // This is complicated by the fact that the order is not preserved for statistics
        sortStatistics(xml2);
        sortStatistics(xml3);

        DBGLOG("Comparing xml2 and xml3");
        checkStringsMatch(xml2, xml3);

        // Check that copy preserves everything it should (and resets what it should)
        // We can't directly compare xml1 with xml2 - not everything is copied
        Owned<IPropertyTree> p2 = createPTreeFromXMLString(xml2);
        p2->removeProp("Statistics/Statistic[@kind='WhenCreated']");
        p2->removeProp("Debug/created_by");
        p2->removeProp("@isClone");
        p2->removeProp("@wuidVersion");
        ASSERT(streq(p2->queryProp("Variables/Variable[@name='one']/@status"), "undefined"));
        p2->setProp("Variables/Variable[@name='one']/@status", "calculated");
        p2->renameProp("/", "W_LOCAL");
        Owned<IPropertyTree> p1 = createPTreeFromXMLString(xml1);
        // Checking that temporaries and tracing were not copied
        p1->removeProp("Temporaries");
        p1->removeProp("Tracing");
        // Checking that variables were reset by the copy
        p1->removeProp("Variables/Variable[@name='one']/rowCount");
        p1->removeProp("Variables/Variable[@name='one']/totalRowCount");
        p1->removeProp("Variables/Variable[@name='one']/Value");
        // Checking that workflow was reset by the copy
        p1->setProp("Workflow/Item[@wfid='1']/@state", "null");
        p1->setProp("Workflow/Item[@wfid='2']/@state", "null");
        p1->setProp("Workflow/Item[@wfid='3']/@state", "reqd");
        toXML(p1, xml1.clear(), 0, XML_Format|XML_SortTags);
        toXML(p2, xml2.clear(), 0, XML_Format|XML_SortTags);
        DBGLOG("Comparing xml1 and xml2");
        checkStringsMatch(xml1, xml2);
        wu.clear();
        factory->deleteWorkUnit(wuid);
    }

    void sortStatistics(StringBuffer &xml)
    {
        Owned<IPropertyTree> p = createPTreeFromXMLString(xml);
        Owned<IPropertyTreeIterator> stats = p->getElements("Statistics/Statistic");
        StringArray unknownAttributes;
        IArrayOf<IPropertyTree> sorted;
        sortElements(stats, "@ts", NULL, NULL, unknownAttributes, sorted);
        p->removeProp("Statistics");
        if (sorted.length())
        {
            Owned<IPTree> parent = createPTree("Statistics");
            ForEachItemIn(idx, sorted)
            {
                parent->addPropTree("Statistic", LINK(&sorted.item(idx)));
            }
            p->addPropTree("Statistics", parent.getClear());
        }
        toXML(p, xml.clear(), 0, XML_Format|XML_SortTags);
    }
    void checkStringsMatch(const char *s1, const char *s2)
    {
        if (!streq(s1, s2))
        {
            int i;
            for (i = 0; s1[i] && s2[i]==s1[i]; i++)
                ;
            DBGLOG("Strings differ:\n%s\n%s\n", s1+i, s2+i);
        }
        ASSERT(streq(s1, s2));
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
        ASSERT(factory->validateRepository(false)==0);
    }

    void testSet()
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        unsigned start = msTick();
        int i;
        for (i = 0; i < testSize; i++)
        {
            Owned<IWorkUnit> wu = factory->updateWorkUnit(wuids.item(i));
            for (int exNum = 0; exNum < 10; exNum++)
            {
                Owned <IWUException> ex = wu->createException();
                ex->setExceptionCode(77 + exNum);
                ex->setExceptionColumn(88 + exNum);
                ex->setExceptionFileName("exfile");
                ex->setExceptionLineNo(99);
                ex->setExceptionMessage("exMessage");
                ex->setExceptionSource("exSource");
                ex->setSeverity(SeverityFatal);
                ex->setTimeStamp("2001");
            }

            wu->addProcess("ptype", "pInstance", 54321, "mylog");
            wu->setAction(WUActionCompile);
            wu->setApplicationValue("app1", "av1", "value", true);
            wu->setApplicationValueInt("app2", "av2", 42, true);
            wu->setIsQueryService(true);
            wu->setClusterName("clustername");
            wu->setDebugValue("debug1", "value", true);
            wu->setDebugValueInt("debug2", 42, true);
            wu->setJobName("jobname");
            wu->setPriority(PriorityClassHigh);
            wu->setPriorityLevel(2) ;
            wu->setRescheduleFlag(true);
            wu->setResultLimit(101);
            wu->setSecurityToken("secret");
            wu->setState(WUStateAborted);
            wu->setStateEx("stateEx");
            wu->setAgentSession(1234567890123);
//            virtual void setStatistic(StatisticCreatorType creatorType, const char * creator, StatisticScopeType scopeType, const char * scope, StatisticKind kind, const char * optDescription, unsigned __int64 value, unsigned __int64 count, unsigned __int64 maxValue, StatsMergeAction mergeAction) = 0;
            wu->setTracingValue("trace1", "tvalue1");
            wu->setTracingValueInt("trace2", 43);
            wu->setUser("user");
            wu->setWuScope("scope");
            wu->setSnapshot("snap");
            wu->setWarningSeverity(1234, SeverityFatal);
        }
        unsigned end = msTick();
        DBGLOG("%u workunits set in %d ms", testSize, end-start);
        start = end;
        SCMStringBuffer s;
        for (i = 0; i < testSize; i++)
        {
            Owned<IConstWorkUnit> wu = factory->openWorkUnit(wuids.item(i), false);
            if (false)
            {
                StringBuffer wuXML;
                exportWorkUnitToXML(wu, wuXML, true, false, false);
                DBGLOG("%s", wuXML.str());
            }
            ASSERT(wu->getExceptionCount() == 10);
            Owned<IConstWUExceptionIterator> exceptions = &wu->getExceptions();
            int exNo = 0;
            ForEach(*exceptions)
            {
                IConstWUException &ex = exceptions->query();
                ASSERT(ex.getExceptionCode()==77 + exNo);
                ASSERT(ex.getExceptionColumn()==88 + exNo);
                ASSERT(streq(ex.getExceptionFileName(s).str(),"exfile"));
                ASSERT(ex.getExceptionLineNo()==99);
                ASSERT(streq(ex.getExceptionMessage(s).str(),"exMessage"));
                ASSERT(streq(ex.getExceptionSource(s).str(),"exSource"));
                ASSERT(ex.getSeverity()==SeverityFatal);
                ASSERT(streq(ex.getTimeStamp(s).str(),"2001"));
                exNo++;
            }

            Owned<IPTreeIterator> processes = wu->getProcesses("ptype", "pInstance");
            ASSERT(processes->first());
            IPTree &process = processes->query();
            ASSERT(process.getPropInt("@pid", 0)==54321);
            ASSERT(streq(process.queryProp("@log"), "mylog"));
            ASSERT(!processes->next());

            ASSERT(wu->getAction() == WUActionCompile);
            ASSERT(streq(wu->getApplicationValue("app1", "av1", s).str(), "value"));
            ASSERT(wu->getApplicationValueInt("app2", "av2", 0) == 42);
            ASSERT(wu->getIsQueryService());
            ASSERT(streq(wu->queryClusterName(),"clustername"));
            ASSERT(streq(wu->getDebugValue("debug1", s).str(), "value"));
            ASSERT(wu->getDebugValueInt("debug2", 0) == 42);
            ASSERT(streq(wu->queryJobName(),"jobname"));
            ASSERT(wu->getPriority()==PriorityClassHigh);
            ASSERT(wu->getPriorityLevel()==2);
            ASSERT(wu->getRescheduleFlag());
            ASSERT(wu->getResultLimit()==101);
            ASSERT(streq(wu->getSecurityToken(s).str(), "secret"));
            ASSERT(wu->getState()==WUStateAborted);
            ASSERT(streq(wu->getStateEx(s).str(), "stateEx"));
            ASSERT(wu->getAgentSession()==1234567890123);
//            virtual void setStatistic(StatisticCreatorType creatorType, const char * creator, StatisticScopeType scopeType, const char * scope, StatisticKind kind, const char * optDescription, unsigned __int64 value, unsigned __int64 count, unsigned __int64 maxValue, StatsMergeAction mergeAction) = 0;
            // Tracing is only retrievable via XML
            const IExtendedWUInterface *ewu = queryExtendedWU(wu);
            ASSERT(ewu);
            IPropertyTree *p = ewu->queryPTree();
            StringBuffer xml;
            ASSERT(p->queryPropTree("Tracing"));
            toXML(p->queryPropTree("Tracing"), xml, 0, XML_SortTags);
            ASSERT(streq(xml, "<Tracing><trace1>tvalue1</trace1><trace2>43</trace2></Tracing>"));

            ASSERT(streq(wu->queryUser(), "user"));
            ASSERT(streq(wu->getWuScope(s).str(), "scope"));
            ASSERT(streq(wu->getSnapshot(s).str(),"snap"));
            ASSERT(wu->getWarningSeverity(1234, SeverityInformation) == SeverityFatal);
        }
        end = msTick();
        DBGLOG("%u workunits reread in %d ms", testSize, end-start);
        start = end;
        for (i = 0; i < testSize; i++)
        {
            Owned<IWorkUnit> wu = factory->updateWorkUnit(wuids.item(i));
            wu->clearExceptions();
        }
        end = msTick();
        DBGLOG("%u * 10 workunit exceptions cleared in %d ms", testSize, end-start);
        start = end;
        for (i = 0; i < testSize; i++)
        {
            Owned<IConstWorkUnit> wu = factory->openWorkUnit(wuids.item(i), false);
            ASSERT(wu->getExceptionCount() == 0);
        }
        end = msTick();
        DBGLOG("%u workunits reread in %d ms", testSize, end-start);
        start = end;
    }

    void testList()
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        bool isDali = streq(factory->queryStoreType(), "Dali");
        unsigned before = factory->numWorkUnits();
        unsigned start = msTick();
        unsigned numIterated = 0;
        Owned<IConstWorkUnitIterator> wus = factory->getWorkUnitsByOwner(NULL, NULL, NULL);
        StringBuffer lastWu;
        ForEach(*wus)
        {
            IConstWorkUnitInfo &wu = wus->query();
            if (lastWu.length() && !isDali)  // Dali does not define the order here
                ASSERT(strcmp(wu.queryWuid(), lastWu) <= 0);
            lastWu.set(wu.queryWuid());
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
        DBGLOG("%d owned workunits listed in %d ms", numIterated, msTick()-start);
        ASSERT(numIterated == (testSize+49)/50);

        // And by non-existent owner...
        wus.setown(factory->getWorkUnitsByOwner("NoSuchWuTestUser0", NULL, NULL));
        start = msTick();
        numIterated = 0;
        ForEach(*wus)
        {
            numIterated++;
        }
        DBGLOG("%d non-existent workunits listed in %d ms", numIterated, msTick()-start);
        ASSERT(numIterated == 0);

        // And by cluster
        wus.setown(factory->getWorkUnitsByCluster("WuTestCluster0", NULL, NULL));
        start = msTick();
        numIterated = 0;
        ForEach(*wus)
        {
            IConstWorkUnitInfo &wu = wus->query();
            ASSERT(streq(wu.queryClusterName(), "WuTestCluster0"));
            numIterated++;
        }
        DBGLOG("%d cluster workunits listed in %d ms", numIterated, msTick()-start);
        ASSERT(numIterated == (testSize+4)/5);

        // And by state
        wus.setown(factory->getWorkUnitsByState(WUStateFailed, NULL, NULL));
        start = msTick();
        numIterated = 0;
        ForEach(*wus)
        {
            IConstWorkUnitInfo &wu = wus->query();
            ASSERT(wu.getState() == WUStateFailed);
            numIterated++;
        }
        DBGLOG("%d failed workunits listed in %d ms", numIterated, msTick()-start);
        ASSERT(numIterated == (testSize+5)/6);

    }
};
StringArray WuTest::wuids;

CPPUNIT_TEST_SUITE_REGISTRATION( WuTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( WuTest, "WuTest" );

#endif
