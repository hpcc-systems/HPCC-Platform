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
#include "jlib.hpp"
#include "jfile.hpp"
#include "jprop.hpp"
#include "jptree.hpp"
#include "jsocket.hpp"
#include "jutil.hpp"
#include "workunit.hpp"
#include "mpbase.hpp"
#include "dllserver.hpp"
#include "daclient.hpp"

#include "daclient.hpp"
#include "dasds.hpp"
#include "dautils.hpp"
#include "danqs.hpp"
#include "dalienv.hpp"
#include "anawu.hpp"

#ifdef _USE_CPPUNIT
#include "workunitservices.hpp"
#include "eclrtl.hpp"
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>
#endif

static unsigned testSize = 1000;

void usage(const char * action = nullptr)
{
    if (action && strieq(action, "info"))
    {
        printf("Usage: wutool info <workunits> <filter>\n"
               "The filter can include the following elements (those followed by * can be repeated):\n"
               "  Which scopes are matched:\n"
               "    scope[<scope-id>]*   - scope to match\n"
               "    stype[<scope-type>]* - scope type to match\n"
               "    id[<id>]*            - id of a scope to to match\n"
               "      scope, stype and id cannot be specified in the same filter\n"
               "\n"
               "    depth[n | low..high] - range of depths to search for a match\n"
               "    source[global|stats|graph|all]*\n"
               "                         - which sources within the workunit to search\n"
               "                           defaults to the optimal sources for the rest of the filter\n"
               "    where[<statistickind> | <statistickind> (=|<|<=|>|>=) value | <statistickind>=low..high]\n"
               "                         - filter by statistic existence or value range\n"
               "\n"
               "  Which scopes are include in the results:\n"
               "    matched[true|false]  - are the matched scopes returned?\n"
               "    nested[<depth>|all]  - what nesting of scopes within a matched scope are in the results\n"
               "                           (defaults to '0' if matched[true] and 'all' if matched[false])\n"
               "    includetype[<scope-type>]*\n"
               "                         - which scope types should be included?\n"
               "\n"
               "  Which information about a scope is reported:\n"
               "    properties[statistics|hints|attributes|scope|all]*\n"
               "    statistic[<statistic-kind>|none|all]*\n"
               "    attribute[<attribute-name>|none|all]*\n"
               "    hint[<hint-name>]*\n"
               "    property[<statistic-kind>|<attribute-name>|<hint-name>]*\n"
               "                         - include property (category is deduced)\n"
               "    measure[<measure>]   - all statistics with a particular measure\n"
               "    version[<version>]   - minimum version to return\n"
               );
    }
    else
    {
        printf("Usage: wutool action [DALISERVER=ip] [CASSANDRASERVER=ip] [option=value]...\n\n"
               "Actions supported are:\n"
               "   list <workunits>    - List workunits\n"
               "   dump <workunits>    - Dump xml for specified workunits\n"
               "   delete <workunits>  - Delete workunits\n"
               "   results <workunits> - Dump results from specified workunits\n"
               "   info <workunits> <filter>\n"
               "                       - Display information from a workunit\n"
               "   analyse <workunit>  - Analyse the workunit to highlight performance issues\n"
               "\n"
               "   archive <workunits> - Archive to xml files [TO=<directory>] [DEL=1] [DELETERESULTS=1] [INCLUDEFILES=1]\n"
               "   restore <filenames> - Restore from xml files [INCLUDEFILES=1]\n"
                "\n"
               "   orphans             - Delete orphaned information from store\n"
               "   cleanup [days=NN]   - Delete workunits older than NN days\n"
               "   validate [fix=1]    - Check contents of workunit repository for errors\n"
               "   clear               - Delete entire workunit repository (requires entire=1 repository=1)\n"
               "   initialize          - Initialize new workunit repository\n"
               "\n"
               "   help <command>      - More help on a command\n"
               "\n"
               "If CASSANDRASERVER is specified, you can specify some connection options including:\n"
               "   CASSANDRA_KEYSPACE   - default is hpcc\n"
               "   CASSANDRA_USER\n"
               "   CASSANDRA_PASSWORD\n"
               "   CASSANDRA_PARTITIONS - used if creating a new repository\n"
               "   CASSANDRA_PREFIXSIZE - used if creating a new repository\n"
               "   TRACELEVEL\n"
               "<workunits> can be specified on commandline, or can be specified using a filter owner=XXXX. If omitted,\n"
               "all workunits will be selected.\n"
                );
    }
}

/* Callback used to output the different scope properties as xml */
class ScopeDumper : public IWuScopeVisitor
{
public:
    virtual void noteStatistic(StatisticKind kind, unsigned __int64 value, IConstWUStatistic & cur) override
    {
        StringBuffer xml;
        SCMStringBuffer curCreator;
        SCMStringBuffer curDescription;
        SCMStringBuffer curFormattedValue;

        StatisticCreatorType curCreatorType = cur.getCreatorType();
        StatisticScopeType curScopeType = cur.getScopeType();
        StatisticMeasure curMeasure = cur.getMeasure();
        unsigned __int64 count = cur.getCount();
        unsigned __int64 max = cur.getMax();
        unsigned __int64 ts = cur.getTimestamp();
        const char * curScope = cur.queryScope();
        cur.getCreator(curCreator);
        cur.getDescription(curDescription, false);
        cur.getFormattedValue(curFormattedValue);

        if (kind != StKindNone)
            xml.append(" kind='").append(queryStatisticName(kind)).append("'");
        xml.append(" value='").append(value).append("'");
        xml.append(" formatted='").append(curFormattedValue).append("'");
        if (curMeasure != SMeasureNone)
            xml.append(" unit='").append(queryMeasureName(curMeasure)).append("'");
        if (curCreatorType != SCTnone)
            xml.append(" ctype='").append(queryCreatorTypeName(curCreatorType)).append("'");
        if (curCreator.length())
            xml.append(" creator='").append(curCreator.str()).append("'");
        if (count != 1)
            xml.append(" count='").append(count).append("'");
        if (max)
            xml.append(" max='").append(value).append("'");
        if (ts)
        {
            xml.append(" ts='");
            formatStatistic(xml, ts, SMeasureTimestampUs);
            xml.append("'");
        }
        if (curDescription.length())
            xml.append(" desc='").append(curDescription.str()).append("'");
        printf(" <attr%s/>\n", xml.str());
    }
    virtual void noteAttribute(WuAttr attr, const char * value)
    {
        StringBuffer xml;
        xml.appendf("<attr kind='%s' value='", queryWuAttributeName(attr));
        encodeXML(value, xml, ENCODE_NEWLINES, (unsigned)-1, true);
        xml.append("'/>");
        printf(" %s\n", xml.str());
    }
    virtual void noteHint(const char * kind, const char * value)
    {
        StringBuffer xml;
        xml.appendf("<attr kind='hint:%s' value='%s'/>", kind, value);
        printf(" %s\n", xml.str());
    }
};


static void process(IConstWorkUnit &w, IProperties *globals, const StringArray & args)
{
    const char *action = globals->queryProp("#action");
    if (!action || stricmp(action, "list")==0)
    {
        printf("%-20s %-10s %-10s %-10s %-10s\n", w.queryWuid(), w.queryClusterName(), w.queryUser(), w.queryJobName(), w.queryStateDesc());
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
    else if (stricmp(action, "analyse")==0)
    {
        analyseWorkunit(&w);
    }
    else if (stricmp(action, "dump")==0)
    {
        StringBuffer xml;
        exportWorkUnitToXML(&w, xml, true, false, true);
        printf("%s\n", xml.str());
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
        IExtendedWUInterface * wuInterface = QUERYINTERFACE(&w, IExtendedWUInterface);
        if (wuInterface && wuInterface->archiveWorkUnit(to.str(), globals->getPropBool("DEL", false), true, globals->getPropBool("DELETERESULTS", false), globals->getPropBool("INCLUDEFILES", false)))
            printf("archived %s\n", wuid.str());
        else
            printf("archive of %s failed\n", wuid.str());
    }
    else if (stricmp(action, "info")==0)
    {
        ScopeDumper dumper;
        WuScopeFilter filter;
        filter.addFilter(args.item(0));
        if (filter.properties == PTnone)
            filter.addOutputProperties(PTall);
        filter.finishedFilter();

        printf("<Workunit wuid=\"%s\">\n", w.queryWuid());
        Owned<IConstWUScopeIterator> iter = &w.getScopeIterator(filter);
        ForEach(*iter)
        {
            if (!filter.onlyIncludeScopes())
            {
                printf("<scope scope='%s' type='%s'>\n", iter->queryScope(), queryScopeTypeName(iter->getScopeType()));
                iter->playProperties(dumper);
                printf("</scope>\n");
            }
            else
                printf("<scope scope='%s' type='%s'/>\n", iter->queryScope(), queryScopeTypeName(iter->getScopeType()));
        }

        printf("</Workunit>\n");
    }
    else
        throwUnexpected();
}


Owned<IProperties> globals;

bool looksLikeWuid(const char * arg)
{
    //Looks like a filename
    if (strchr(arg,'.') || strchr(arg,PATHSEPCHAR))
        return false;

    if (arg[0]!='W' && arg[0]!='w')
        return false;

    if (isdigit(arg[1]) || (arg[1] == '*'))
        return true;
    return false;
}

int main(int argc, const char *argv[])
{
    int ret = 0;
    InitModuleObjects();
    unsigned count=0;
    globals.setown(createProperties("wutool.ini", true));
    const char *action = NULL;
    StringArray wuids;
    StringArray args;
    for (int i = 1; i < argc; i++)
    {
        const char * arg = argv[i];
        if (strchr(arg,'=') && !strchr(arg, '['))
            globals->loadProp(arg);
        else if (looksLikeWuid(arg))
            wuids.append(arg);
        else if (action)
        {
            args.append(arg);
        }
        else
        {
            action = argv[i];
            globals->setProp("#action", argv[i]);
        }
    }
    if (!action || !*action)
    {
        usage(nullptr);
        _exit(4);
    }
    if (strieq(action, "help"))
    {
        if (args.empty())
            usage(nullptr);
        else
            usage(args.item(0));
        _exit(4);
    }
    try
    {
        StringBuffer cassandraServer;
        bool serverSpecified = false;
        if (globals->getProp("CASSANDRASERVER", cassandraServer))
        {
            // If they explicitly specify a cassandra server, use it even if the info in the environment in dali does not indicate there is a cassandra associated
            // Conversely, if they explicitly specify "none" then don't use cassandra even if there is one specified in the dali environment...
            if (!strieq(cassandraServer, "none"))
            {
                Owned<IPTree> pluginInfo = createPTreeFromXMLString(
                      "<WorkUnitsServer pluginName='cassandraembed' entrypoint='createWorkUnitFactory'>"
                        "<Option name='server' value='.'/>"
                        "<Option name='randomWuidSuffix' value='4'/>"
                        "<Option name='traceLevel' value='0'/>"
                        "<Option name='partitions' value='0'/>"
                        "<Option name='prefixSize' value='0'/>"
                        "<Option name='keyspace' value='hpcc'/>"
                        "<Option name='user' value=''/>"
                        "<Option name='password' value=''/>"
                      "</WorkUnitsServer>");
                pluginInfo->setProp("Option[@name='server']/@value", cassandraServer.str());
                pluginInfo->setPropInt("Option[@name='traceLevel']/@value", globals->getPropInt("tracelevel", 0));
                StringBuffer keySpace,user,password;
                if (globals->getProp("CASSANDRA_KEYSPACE", keySpace))
                    pluginInfo->setProp("Option[@name='keyspace']/@value", keySpace.str());
                if (globals->getProp("CASSANDRA_USER", user))
                    pluginInfo->setProp("Option[@name='user']/@value", user.str());
                else
                    pluginInfo->removeProp("Option[@name='user']");
                if (globals->getProp("CASSANDRA_PASSWORD", password))
                    pluginInfo->setProp("Option[@name='password']/@value", password.str());
                else
                    pluginInfo->removeProp("Option[@name='password']");
                int partitions = globals->getPropInt("CASSANDRA_PARTITIONS", -1);
                if (partitions != -1)
                    pluginInfo->setPropInt("Option[@name='partitions']/@value", partitions);
                else
                    pluginInfo->removeProp("Option[@name='partitions']");
                int prefixSize = globals->getPropInt("CASSANDRA_PREFIXSIZE", -1);
                if (prefixSize != -1)
                    pluginInfo->setPropInt("Option[@name='prefixSize']/@value", prefixSize);
                else
                    pluginInfo->removeProp("Option[@name='prefixSize']");
                setWorkUnitFactory((IWorkUnitFactory *) loadPlugin(pluginInfo));
                serverSpecified = true;
            }
        }

        StringBuffer daliServers;
        if (globals->getProp("DALISERVER", daliServers))
        {
            Owned<IGroup> serverGroup = createIGroup(daliServers.str(), DALI_SERVER_PORT);
            initClientProcess(serverGroup,DCR_Other);
            setPasswordsFromSDS();
        }
        else if (!serverSpecified)
        {
            printf("No server specified (at least one of daliserver or cassandraserver required)\n");
            _exit(4);
        }
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
#ifdef _USE_CPPUNIT
        if (stricmp(action, "-selftest")==0)
        {
            StringBuffer testName;
            if (!globals->getProp("test", testName))
                testName.set("WuTool");
            testSize = globals->getPropInt("testSize", 100);
            queryStderrLogMsgHandler()->setMessageFields(MSGFIELD_time | MSGFIELD_milliTime | MSGFIELD_prefix);
            CppUnit::TextUi::TestRunner runner;
            CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry(testName.str());
            runner.addTest( registry.makeTest() );
            ret = runner.run( "", false );
        }
        else
#endif
        if (stricmp(action, "validate")==0)
        {
            bool fix = globals->getPropBool("fix", false);
            unsigned errors = factory->validateRepository(fix);
            printf("%u errors %s\n", errors, (fix && errors) ? "fixed" : "found");
        }
        else if (stricmp(action, "clear")==0)
        {
            if (globals->getPropBool("entire", false) && globals->getPropBool("repository", false))
            {
                factory->deleteRepository(false);
                printf("Repository deleted\n");
            }
            else
                printf("You need to specify entire=1 and repository=1 to delete entire repository\n");
        }
        else if (stricmp(action, "initialize")==0)
        {
            factory->createRepository();
            printf("Repository created\n");
        }
        else if (stricmp(action, "orphans")==0 || stricmp(action, "cleanup")==0)
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
                    Owned<IConstWorkUnit> w = factory->openWorkUnit(wuid);
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
                            Owned<IConstWorkUnit> w = factory->openWorkUnit(wuid.str());
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
        else if (strieq(action, "restore"))
        {
            if (wuids.length())
            {
                usage();
                exit(4);
            }
            if (!args.length())
            {
                printf("One or more workunit files must be specified\n");
                exit(4);
            }
            ForEachItemIn(idx, args)
            {
                StringBuffer base, wuid, ext;
                splitFilename(args.item(idx), &base, &base, &wuid, &ext, true);
                if (streq(ext, ".xml"))
                {
                    if (base.length()==0)
                        base.append('.');
                    if (factory->restoreWorkUnit(base.str(), wuid, globals->getPropBool("INCLUDEFILES", false)))
                        printf("restored %s\n", wuid.str());
                    else
                        printf("failed to restore %s\n", wuid.str());
                }
                else
                    printf("Ignoring file %s - extension is not '.xml'\n", args.item(idx));
            }
        }
        else if (strieq(action, "help"))
        {
            usage();
        }
        else if (strieq(action, "list") || strieq(action, "dump") || strieq(action, "results") || strieq(action, "delete") || strieq(action, "archive") || strieq(action, "info") || strieq(action, "analyse"))
        {
            if (strieq(action, "info") && args.empty())
                args.append("source[all],properties[all]");

            if (wuids.length())
            {
                ForEachItemIn(idx, wuids)
                {
                    const char * wuid = wuids.item(idx);
                    const char * star = strchr(wuid, '*');
                    if (star)
                    {
                        WUSortField filters[2];
                        MemoryBuffer filterbuf;
                        filters[0] = WUSFwildwuid;
                        filterbuf.append(wuid);
                        filters[1] = WUSFterm;
                        Owned<IConstWorkUnitIterator> iter = factory->getWorkUnitsSorted((WUSortField) (WUSFwuid), filters, filterbuf.bufferBase(), 0, INT_MAX, NULL, NULL);

                        ForEach(*iter)
                        {
                            Owned<IConstWorkUnit> workunit = factory->openWorkUnit(iter->query().queryWuid());
                            if (workunit)
                                process(*workunit, globals, args);
                        }
                    }
                    else
                    {
                        Owned<IConstWorkUnit> workunit = factory->openWorkUnit(wuid);
                        if (workunit)
                            process(*workunit, globals, args);
                    }
                }
            }
            else
            {
                if (strieq(action, "delete") && !globals->getPropBool(("FORCE")))
                {
                    printf("Specify FORCE=1 to delete workunits via filter");
                    _exit(4);
                }
                ret = 1;  // Assume there will be no matches
                Owned<IConstWorkUnitIterator> it = factory->getWorkUnitsByOwner(globals->queryProp("OWNER"));
                ForEach(*it)
                {
                    IConstWorkUnitInfo& wi = it->query();
                    Owned<IConstWorkUnit> w = factory->openWorkUnit(wi.queryWuid());
                    if (w)
                    {
                        process(*w, globals, args);
                        ret = 0; // There was at least one match
                    }
                }
            }
        }
        else
        {
            usage();
            _exit(4);
        }
    }
    catch (IException *E)
    {
        StringBuffer m;
        printf("Error: %s\n", E->errorMessage(m).str());
        E->Release();
        ret = 2;
    }
    closeDllServer();
    closeEnvironment();
    clientShutdownWorkUnit();
    closedownClientProcess();   // dali client closedown
    if (queryActiveTimer())
    {
        queryActiveTimer()->printTimings();
        queryActiveTimer()->reset();
    }
    releaseAtoms();
    return ret;
}

#ifdef _USE_CPPUNIT
#include "unittests.hpp"
inline int max(int a, int b)
{
    return a > b ? a : b;
}
inline int min(int a, int b)
{
    return a < b ? a : b;
}
class WuTool : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(WuTool);
        CPPUNIT_TEST(testInit);
        CPPUNIT_TEST(testCreate);
        CPPUNIT_TEST(testGlobal);
        CPPUNIT_TEST(testValidate);
        CPPUNIT_TEST(testList);
        CPPUNIT_TEST(testList2);
        CPPUNIT_TEST(testListByAppValue);
        CPPUNIT_TEST(testListByAppValueWild);
        CPPUNIT_TEST(testListByFilesRead);
        CPPUNIT_TEST(testListByFilesWritten);
        CPPUNIT_TEST(testSortByThorTime);
        CPPUNIT_TEST(testSet);
        CPPUNIT_TEST(testResults);
        CPPUNIT_TEST(testWorkUnitServices);
        CPPUNIT_TEST(testDelete);
        CPPUNIT_TEST(testCopy);
        CPPUNIT_TEST(testQuery);
        CPPUNIT_TEST(testGraph);
        CPPUNIT_TEST(testGraphProgress);
    CPPUNIT_TEST_SUITE_END();
protected:
    static StringArray wuids;
    void testInit()
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        if (globals->getPropBool("entire", false) && globals->getPropBool("repository", false))
        {
            factory->deleteRepository(false);
            factory->createRepository();
            DBGLOG("Repository recreated\n");
        }
    }
    void testCreate()
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        unsigned before = factory->numWorkUnits();
        unsigned start = msTick();
        for (int i = 0; i < testSize; i++)
        {
            VStringBuffer userId("WuTestUser%02d", i % 50);
            VStringBuffer clusterName("WuTestCluster%d", i % 5);
            VStringBuffer jobName("WuTest job %d", i % 3);
            Owned<IWorkUnit>wu = factory->createWorkUnit("WuTest", NULL, NULL, NULL);
            if (i % 6)
                wu->setState(WUStateCompleted);
            else
                wu->setState(WUStateScheduled);
            wu->setUser(userId);
            wu->setClusterName(clusterName);
            if (i % 3)
                wu->setJobName(jobName);
            wu->setStatistic(SCTthor, "thor", SSTgraph, "graph1", StTimeElapsed, "Total thor time", ((i+2)/2) * 1000000, 1, 0, StatsMergeReplace);
            wu->setApplicationValue("appname", "userId", userId.str(), true);
            wu->setApplicationValue("appname2", "clusterName", clusterName.str(), true);
            wuids.append(wu->queryWuid());

            // We should really be doing a noteFileRead here but the API is such a pain that we'll do it this way
            IPropertyTree *p = queryExtendedWU(wu)->queryPTree();
            VStringBuffer fileinfo(" <FilesRead>"
                "  <File name='myfile%02d' useCount='2' cluster = 'mycluster'/>"
                "  <File name='mysuperfile' useCount='2' cluster = 'mycluster'>"
                "   <Subfile name='myfile%02d'/>"
                "  </File>"
                " </FilesRead>", i % 10, i % 10);
            p->setPropTree("FilesRead", createPTreeFromXMLString(fileinfo));
            wu->noteFileRead(NULL); // Make sure we notice that it was modified

            VStringBuffer myFileW("myfilewritten%02d", i % 10);
            wu->addFile(myFileW, NULL, i % 3, WUFileStandard, NULL);
        }
        unsigned after = factory->numWorkUnits();
        DBGLOG("%u workunits created in %d ms (%d total)", testSize, msTick()-start, after);
        ASSERT(after-before==testSize);
        ASSERT(wuids.length() == testSize);
    }

    void testValidate()
    {
        unsigned start = msTick();
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        ASSERT(factory->validateRepository(false)==0);
        DBGLOG("Repository validated in %d ms", msTick()-start);
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
                "         totalThorTime=''"
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
                " <FilesRead>"
                "  <File name='myfile' useCount='2' cluster = 'mycluster'/>"
                "  <File name='mysuperfile' useCount='2' cluster = 'mycluster'>"
                "   <Subfile name='myfile'/>"
                "  </File>"
                "</FilesRead>"
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
                " <State>completed</State>"
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
        queryExtendedWU(createWu)->copyWorkUnit(embeddedWU, true, true);
        createWu->setState(WUStateCompleted);
        exportWorkUnitToXML(createWu, xml2, false, false, false);
        createWu->commit();
        createWu.clear();

        // Now try to re-read
        Owned<IConstWorkUnit> wu = factory->openWorkUnit(wuid);
        ASSERT(streq(wu->queryWuid(), wuid));
        ASSERT(streq(wu->queryJobName(), embeddedWU->queryJobName()));
        exportWorkUnitToXML(wu, xml3, false, false, false);
        // Check that writing to/reading from the server leaves unmodified
        // This is complicated by the fact that the order is not preserved for statistics
        sortStatistics(xml2);
        sortStatistics(xml3);
        if (wu->getState() != WUStateCompleted)
            DBGLOG("Not completed");
        DBGLOG("Comparing xml2 and xml3");
        checkStringsMatch(xml2, xml3);

        // Check that copy preserves everything it should (and resets what it should)
        // We can't directly compare xml1 with xml2 - not everything is copied
        Owned<IPropertyTree> p2 = createPTreeFromXMLString(xml2);
        p2->removeProp("Statistics/Statistic[@kind='WhenCreated']");
        p2->removeProp("Debug/created_by");
        p2->removeProp("@isClone");
        p2->removeProp("@wuidVersion");
        p2->removeProp("@distributedAccessToken");
        ASSERT(streq(p2->queryProp("Variables/Variable[@name='one']/@status"), "undefined"));
        p2->setProp("Variables/Variable[@name='one']/@status", "calculated");
        p2->renameProp("/", "W_LOCAL");
        Owned<IPropertyTree> p1 = createPTreeFromXMLString(xml1);
        // Checking that temporaries and tracing were not copied
        p1->removeProp("Temporaries");
        p1->removeProp("Tracing");
        p1->removeProp("FilesRead"); // Check this is not copied
        // Checking that variables were reset by the copy
        p1->removeProp("Variables/Variable[@name='one']/rowCount");
        p1->removeProp("Variables/Variable[@name='one']/totalRowCount");
        p1->removeProp("Variables/Variable[@name='one']/Value");
        // Checking that results were reset by the copy
        p1->setProp("Results/Result[@name='Result 1']/@status", "undefined");
        p1->removeProp("Results/Result[@name='Result 1']/rowCount");
        p1->removeProp("Results/Result[@name='Result 1']/totalRowCount");
        p1->removeProp("Results/Result[@name='Result 1']/Value");
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

    void testQuery()
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        Owned<IWorkUnit> createWu = factory->createWorkUnit("WuTest", NULL, NULL, NULL);
        StringBuffer wuid(createWu->queryWuid());
        {
            Owned<IWUQuery> query = createWu->updateQuery();
            ASSERT(query);
            query->setQueryText("Hello");
            query->setQueryName("qname");
            query->setQueryMainDefinition("fred");
            query->setQueryType(QueryTypeEcl);
            query->addAssociatedFile(FileTypeCpp, "myfile", "nosuchurl.co.ggg", "Description", 53, 3, 4);
            createWu->setState(WUStateCompleted);
            createWu.clear();
        }

        Owned<IConstWorkUnit> wu = factory->openWorkUnit(wuid);
        ASSERT(streq(wu->queryWuid(), wuid));
        Owned<IConstWUQuery> query = wu->getQuery();
        ASSERT(query);
        SCMStringBuffer s;
        ASSERT(streq(query->getQueryText(s).str(), "Hello"));
        ASSERT(streq(query->getQueryName(s).str(), "qname"));
        ASSERT(streq(query->getQueryMainDefinition(s).str(),"fred"));
        ASSERT(query->getQueryType()==QueryTypeEcl);
        Owned <IConstWUAssociatedFile> file = query->getAssociatedFile(FileTypeCpp, 0);
        ASSERT(file);
        ASSERT(streq(file->getDescription(s).str(), "Description"));
        ASSERT(streq(file->getName(s).str(), "myfile"));
        ASSERT(file->getCrc()==53);
        ASSERT(file->getType()==FileTypeCpp);
        ASSERT(file->getMinActivityId()==3);
        ASSERT(file->getMaxActivityId()==4);
        ASSERT(streq(file->getIp(s).str(), "nosuchurl.co.ggg"));
        query.clear();
        wu.clear();

        Owned<IWorkUnit> lockedWu = factory->updateWorkUnit(wuid);
        Owned<IWUQuery> uQuery = lockedWu->updateQuery();
        ASSERT(uQuery);
        uQuery->removeAssociatedFiles();
        uQuery.clear();
        lockedWu.clear();

        factory->deleteWorkUnit(wuid);
    }

    void testGraph()
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        Owned<IWorkUnit> createWu = factory->createWorkUnit("WuTest", NULL, NULL, NULL);
        StringBuffer wuid(createWu->queryWuid());
        createWu->createGraph("Graph1", "graphLabel", GraphTypeActivities, createPTreeFromXMLString("<graph/>"), 0);
        createWu->createGraph("Graph2", "graphLabel", GraphTypeActivities, createPTreeFromXMLString("<graph/>"), 1);
        createWu->createGraph("Graph3", "graphLabel", GraphTypeEcl, createPTreeFromXMLString("<graph/>"), 2);
        createWu->setState(WUStateCompleted);
        createWu->commit();
        createWu.clear();
        // Now try to reread a single graph....
        SCMStringBuffer s;
        Owned<IConstWorkUnit> wu = factory->openWorkUnit(wuid);
        ASSERT(streq(wu->queryWuid(), wuid));
        Owned<IConstWUGraph> graph = wu->getGraph("Graph1");
        ASSERT(graph != NULL);
        ASSERT(graph->getType() == GraphTypeActivities);
        ASSERT(streq(graph->getName(s).str(),"Graph1"));
        ASSERT(streq(graph->getLabel(s).str(),"graphLabel"));
        ASSERT(streq(graph->getXGMML(s, false).str(), "<graph/>\n"));

        // Then the lightweight meta....
        wu.setown(factory->openWorkUnit(wuid));
        ASSERT(streq(wu->queryWuid(), wuid));
        Owned<IConstWUGraphMetaIterator> it = &wu->getGraphsMeta(GraphTypeActivities);
        unsigned numIterated = 0;
        ForEach (*it)
        {
            ASSERT(it->query().getType() == GraphTypeActivities);
            ASSERT(streq(graph->getLabel(s).str(),"graphLabel"));
            numIterated++;
        }
        ASSERT(numIterated==2);

        wu.setown(factory->openWorkUnit(wuid));
        ASSERT(streq(wu->queryWuid(), wuid));
        it.setown(&wu->getGraphsMeta(GraphTypeAny));
        numIterated = 0;
        ForEach (*it)
        {
            ASSERT(streq(it->query().getLabel(s).str(),"graphLabel"));
            numIterated++;
        }
        ASSERT(numIterated==3);

        // then the heavy meta
        wu.setown(factory->openWorkUnit(wuid));
        ASSERT(streq(wu->queryWuid(), wuid));
        Owned<IConstWUGraphIterator> it2 = &wu->getGraphs(GraphTypeActivities);
        numIterated = 0;
        ForEach (*it2)
        {
            ASSERT(it2->query().getType() == GraphTypeActivities);
            ASSERT(streq(it2->query().getLabel(s).str(),"graphLabel"));
            ASSERT(streq(it2->query().getXGMML(s, false).str(), "<graph/>\n"));
            numIterated++;
        }
        ASSERT(numIterated==2);

        wu.setown(factory->openWorkUnit(wuid));
        ASSERT(streq(wu->queryWuid(), wuid));
        it2.setown(&wu->getGraphs(GraphTypeAny));
        numIterated = 0;
        ForEach (*it2)
        {
            ASSERT(streq(it2->query().getLabel(s).str(),"graphLabel"));
            ASSERT(streq(it2->query().getXGMML(s, false).str(), "<graph/>\n"));
            numIterated++;
        }
        ASSERT(numIterated==3);

        // Light then heavy from a single wu
        wu.setown(factory->openWorkUnit(wuid));
        ASSERT(streq(wu->queryWuid(), wuid));
        it.setown(&wu->getGraphsMeta(GraphTypeActivities));
        numIterated = 0;
        ForEach (*it)
        {
            ASSERT(it->query().getType() == GraphTypeActivities);
            ASSERT(streq(it->query().getLabel(s).str(),"graphLabel"));
            numIterated++;
        }
        ASSERT(numIterated==2);

        it2.setown(&wu->getGraphs(GraphTypeActivities));
        numIterated = 0;
        ForEach (*it2)
        {
            ASSERT(it2->query().getType() == GraphTypeActivities);
            ASSERT(streq(it2->query().getLabel(s).str(),"graphLabel"));
            ASSERT(streq(it2->query().getXGMML(s, false).str(), "<graph/>\n"));
            numIterated++;
        }
        ASSERT(numIterated==2);

        // Heavy then light from a single wu
        wu.setown(factory->openWorkUnit(wuid));
        ASSERT(streq(wu->queryWuid(), wuid));
        it2.setown(&wu->getGraphs(GraphTypeActivities));
        numIterated = 0;
        ForEach (*it2)
        {
            ASSERT(it2->query().getType() == GraphTypeActivities);
            ASSERT(streq(it2->query().getLabel(s).str(),"graphLabel"));
            ASSERT(streq(it2->query().getXGMML(s, false).str(), "<graph/>\n"));
            numIterated++;
        }
        ASSERT(numIterated==2);

        it.setown(&wu->getGraphsMeta(GraphTypeActivities));
        numIterated = 0;
        ForEach (*it)
        {
            ASSERT(it->query().getType() == GraphTypeActivities);
            ASSERT(streq(it->query().getLabel(s).str(),"graphLabel"));
            numIterated++;
        }
        ASSERT(numIterated==2);
        wu.clear();
        factory->deleteWorkUnit(wuid);
    }
    void testGraphProgress()
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        Owned<IWorkUnit> createWu = factory->createWorkUnit("WuTest", NULL, NULL, NULL);
        StringBuffer wuid(createWu->queryWuid());
        createWu->createGraph("graph1", "graphLabel", GraphTypeActivities, createPTreeFromXMLString("<graph><node id='1'/></graph>"), 1);
        createWu->setState(WUStateCompleted);
        createWu->commit();
        createWu.clear();
        Owned<IConstWorkUnit> wu = factory->openWorkUnit(wuid);
        ASSERT(streq(wu->queryWuid(), wuid));

        SCMStringBuffer s;
        s.set("Not empty");
        WUGraphIDType subid = 10;
        bool ret = wu->getRunningGraph(s, subid);
        ASSERT(!ret);
        ASSERT(wu->queryGraphState("graph1")==WUGraphUnknown);
        ASSERT(wu->queryNodeState("graph1", 1)==WUGraphUnknown);

        wu->setGraphState("graph1",WUGraphRunning);
        ASSERT(wu->queryGraphState("graph1")==WUGraphRunning);

        wu->setNodeState("graph1", 1, WUGraphRunning);
        ASSERT(wu->queryNodeState("graph1", 1)==WUGraphRunning);
        ret = wu->getRunningGraph(s, subid);
        ASSERT(ret);
        ASSERT(streq(s.str(), "graph1"));
        ASSERT(subid==1);

        wu->setNodeState("graph1", 1, WUGraphComplete);
        ASSERT(wu->queryNodeState("graph1", 1)==WUGraphComplete);
        ret = wu->getRunningGraph(s, subid);
        ASSERT(!ret);

        Owned<IWUGraphStats> progress = wu->updateStats("graph1", SCThthor, queryStatisticsComponentName(), 0, 1);
        IStatisticGatherer & stats = progress->queryStatsBuilder();
        {
            StatsSubgraphScope subgraph(stats, 1);
            stats.addStatistic(StTimeElapsed, 5000);
        }
        progress.clear();


        ASSERT(wu->queryGraphState("graph1")==WUGraphRunning);
        wu->clearGraphProgress();
        ASSERT(wu->queryGraphState("graph1")==WUGraphUnknown);
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
            Owned<IConstWorkUnit> wu = factory->openWorkUnit(wuids.item(i));
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
            ASSERT(streq(wu->queryWuScope(), "scope"));
            ASSERT(streq(wu->getSnapshot(s).str(),"snap"));
            ASSERT(wu->getWarningSeverity(1234, SeverityInformation) == SeverityFatal);

            ASSERT(wu->getSourceFileCount()==2);
            Owned<IPropertyTreeIterator> sourceFiles = &wu->getFilesReadIterator();
            ForEach(*sourceFiles)
            {
                IPTree &file = sourceFiles->query();
                ASSERT(file.getPropInt("@useCount") == 2);
                if (streq(file.queryProp("@name"), "mysuperfile"))
                {
                    ASSERT(strncmp(file.queryProp("Subfile/@name"), "myfile", 6)==0);
                }
                else
                {
                    ASSERT(strncmp(file.queryProp("@name"), "myfile", 6)==0);
                    ASSERT(!file.hasProp("Subfile"));
                }
            }
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
            Owned<IConstWorkUnit> wu = factory->openWorkUnit(wuids.item(i));
            ASSERT(wu->getExceptionCount() == 0);
        }
        end = msTick();
        DBGLOG("%u workunits reread in %d ms", testSize, end-start);
        start = end;
    }

    void testResults()
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        unsigned start = msTick();
        int i;
        for (i = 0; i < testSize; i++)
        {
            Owned<IWorkUnit> wu = factory->updateWorkUnit(wuids.item(i));
            Owned<IWUResult> result = wu->updateResultByName("Result 1");
            Owned<IWUResult> temporary = wu->updateTemporaryByName("Temporary 1");
            result->setResultScalar(true);
            result->setResultSequence(0);
            result->setResultInt(i);
        }
        unsigned end = msTick();
        DBGLOG("%u workunits results updated in %d ms", testSize, end-start);
        start = msTick();
        for (i = 0; i < testSize; i++)
        {
            Owned<IConstWorkUnit> wu = factory->openWorkUnit(wuids.item(i));
            ASSERT(wu->getResultCount()==1);
            Owned<IConstWUResult> result = wu->getResultByName("Result 1");
            ASSERT(result);
            ASSERT(result->isResultScalar());
            ASSERT(result->getResultInt()==i);
            result.setown(wu->getTemporaryByName("Temporary 1"));
            ASSERT(result);
            ASSERT(result->getResultStatus()==ResultStatusUndefined);
        }
        end = msTick();
        DBGLOG("%u workunits results checked in %d ms", testSize, end-start);
    }

    void testList()
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        bool isDali = streq(factory->queryStoreType(), "Dali");
        unsigned before = factory->numWorkUnits();
        unsigned start = msTick();
        unsigned numIterated = 0;
        Owned<IConstWorkUnitIterator> wus;
        wus.setown(factory->getWorkUnitsByOwner(NULL, NULL, NULL));
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
        start = msTick();
        wus.setown(factory->getWorkUnitsByOwner("WuTestUser00", NULL, NULL));
        numIterated = 0;
        ForEach(*wus)
        {
            IConstWorkUnitInfo &wu = wus->query();
            ASSERT(streq(wu.queryUser(), "WuTestUser00"));
            numIterated++;
        }
        DBGLOG("%d owned workunits listed in %d ms", numIterated, msTick()-start);
        ASSERT(numIterated == (testSize+49)/50);

        // And by non-existent owner...
        start = msTick();
        wus.setown(factory->getWorkUnitsByOwner("NoSuchWuTestUser", NULL, NULL));
        numIterated = 0;
        ForEach(*wus)
        {
            numIterated++;
        }
        DBGLOG("%d non-existent workunits listed in %d ms", numIterated, msTick()-start);
        ASSERT(numIterated == 0);

        // Now by owner via generic mechanism
        WUSortField sortByOwner[] = { WUSFuser, WUSFstate, WUSFterm };
        start = msTick();
        wus.setown(factory->getWorkUnitsSorted((WUSortField) (WUSFwuid | WUSFreverse), sortByOwner, "WuTestUser00\0completed", 0, 10000, NULL, NULL));
        unsigned numCompleted = 0;
        ForEach(*wus)
        {
            IConstWorkUnitInfo &wu = wus->query();
            ASSERT(streq(wu.queryUser(), "WuTestUser00"));
            ASSERT(wu.getState()==WUStateCompleted);
            numCompleted++;
        }
        DBGLOG("%d owned,completed workunits listed the hard way in %d ms", numCompleted, msTick()-start);
        ASSERT(numCompleted > 0);  // Not sure what the exact answer should be! Around 5/6 of 1/50th ?

        start = msTick();
        wus.setown(factory->getWorkUnitsSorted((WUSortField) (WUSFwuid | WUSFreverse), sortByOwner, "WuTestUser00\0scheduled", 0, 10000, NULL, NULL));
        unsigned numScheduled = 0;
        ForEach(*wus)
        {
            IConstWorkUnitInfo &wu = wus->query();
            ASSERT(streq(wu.queryUser(), "WuTestUser00"));
            ASSERT(wu.getState()==WUStateScheduled);
            numScheduled++;
        }
        DBGLOG("%d owned,scheduled workunits listed the hard way in %d ms", numScheduled, msTick()-start);
        ASSERT(numScheduled > 0);
        ASSERT(numScheduled + numCompleted == (testSize+49)/50);

        // Use multiple states
        DBGLOG("Testing multiple states");
        start = msTick();
        wus.setown(factory->getWorkUnitsSorted((WUSortField) (WUSFwuid | WUSFreverse), sortByOwner, "WuTestUser00\0completed|scheduled", 0, 10000, NULL, NULL));
        unsigned numCompleted2 = 0;
        unsigned numScheduled2 = 0;
        ForEach(*wus)
        {
            IConstWorkUnitInfo &wu = wus->query();
            ASSERT(streq(wu.queryUser(), "WuTestUser00"));
            ASSERT((wu.getState()==WUStateCompleted) || (wu.getState()==WUStateScheduled));
            if ((wu.getState()==WUStateCompleted))
                numCompleted2++;
            else
                numScheduled2++;
        }
        DBGLOG("%d owned,completed and %d owned,scheduled workunits listed via multiple in %d ms", numCompleted2, numScheduled2, msTick()-start);
        ASSERT(numCompleted == numCompleted2);
        ASSERT(numScheduled == numScheduled2);

        // Use multiple states only
        DBGLOG("Testing multiple states only");
        start = msTick();
        WUSortField filterByState[] = { WUSFstate, WUSFterm };
        wus.setown(factory->getWorkUnitsSorted((WUSortField) (WUSFwuid | WUSFreverse), filterByState, "completed|scheduled", 0, 10000, NULL, NULL));
        unsigned numCompleted3 = 0;
        unsigned numScheduled3 = 0;
        ForEach(*wus)
        {
            IConstWorkUnitInfo &wu = wus->query();
            ASSERT((wu.getState()==WUStateCompleted) || (wu.getState()==WUStateScheduled));
            if (strlen(wu.queryUser()) > 10 && memcmp(wu.queryUser(), "WuTestUser", 10)==0)  // Ignore any wu's not part of the test set
            {
                if ((wu.getState()==WUStateCompleted))
                    numCompleted3++;
                else
                    numScheduled3++;
            }
        }
        DBGLOG("%d completed and %d scheduled workunits listed via multiple in %d ms", numCompleted3, numScheduled3, msTick()-start);
        ASSERT(numCompleted3 >= numCompleted2);
        ASSERT(numScheduled3 >= numScheduled2);
        ASSERT(numCompleted3 + numScheduled3 == testSize);



        // Get Scheduled Workunits
        start = msTick();
        wus.setown(factory->getScheduledWorkUnits(NULL, NULL));
        numIterated = 0;
        ForEach(*wus)
        {
            IConstWorkUnitInfo &wu = wus->query();
            ASSERT(wu.getState() == WUStateScheduled);
            numIterated++;
        }
        DBGLOG("%d scheduled workunits listed in %d ms", numIterated, msTick()-start);
        ASSERT(numIterated == (testSize+5)/6);

        // Get unique users
        if (!isDali)
        {
            start = msTick();
            StringArray users;
            factory->getUniqueValues(WUSFuser, "WUTest", users);
            int expected = testSize < 50 ? testSize : 50;
            ASSERT(users.length() == expected);
            ForEachItemIn(idx, users)
            {
                VStringBuffer name("WuTestUser%02d", idx);
                ASSERT(streq(users.item(idx), name));
            }
            DBGLOG("%d unique users listed in %d ms", users.length(), msTick()-start);
        }

        // Get by hard filter and wuid range
        WUSortField filterBySchedWuid[] = { WUSFstate, /*WUSFwildwuid, WUSFwuid, WUSFwuidhigh, */ WUSFterm };
        start = msTick();
        wus.setown(factory->getWorkUnitsSorted((WUSortField) (WUSFwuid | WUSFreverse), filterBySchedWuid, "scheduled\0W*\0W1\0XA", 0, 100000, NULL, NULL));
        numIterated = 0;
        ForEach(*wus)
        {
            IConstWorkUnitInfo &wu = wus->query();
            ASSERT(wu.getState() == WUStateScheduled);
            numIterated++;
        }
        DBGLOG("%d scheduled workunits listed the hard way in %d ms", numIterated, msTick()-start);
        ASSERT(numIterated == (testSize+5)/6);

        // Get by wuid range only
        WUSortField filterByWuid[] = { WUSFwuid, WUSFwuidhigh, WUSFterm };
        start = msTick();
        StringAttr prevValue;
        wus.setown(factory->getWorkUnitsSorted((WUSortField) (WUSFwuid | WUSFreverse), filterByWuid, "W1\0W3", 0, 10000, NULL, NULL));
        numIterated = 0;
        ForEach(*wus)
        {
            IConstWorkUnitInfo &wu = wus->query();
            if (numIterated)
                ASSERT(strcmp(wu.queryWuid(), prevValue)<0);
            prevValue.set(wu.queryWuid());
            numIterated++;
        }
        DBGLOG("%d ranged workunits listed the hard way in %d ms", numIterated, msTick()-start);
        ASSERT_EQUAL(before-(isDali?1:0), numIterated); // Dali includes the global workunit in the count, cassandra does not. This filter will not include the global wu

        // Check ascending wuids
        WUSortField filterByCluster[] = { WUSFcluster, WUSFterm };
        start = msTick();
        wus.setown(factory->getWorkUnitsSorted(WUSFwuid, filterByCluster, "WuTestCluster0", 0, 10000, NULL, NULL));
        numIterated = 0;
        ForEach(*wus)
        {
            IConstWorkUnitInfo &wu = wus->query();
            ASSERT(streq(wu.queryClusterName(), "WuTestCluster0"));
            if (numIterated)
                ASSERT(strcmp(wu.queryWuid(), prevValue)>0);
            prevValue.set(wu.queryWuid());
            numIterated++;
        }
        DBGLOG("%d workunits listed by cluster, ascending wuid in %d ms", numIterated, msTick()-start);
        ASSERT(numIterated == (testSize+4)/5);

        // Check local sort
        start = msTick();
        wus.setown(factory->getWorkUnitsSorted((WUSortField)(WUSFstate|WUSFreverse), filterByCluster, "WuTestCluster0", 0, 10000, NULL, NULL));
        numIterated = 0;
        ForEach(*wus)
        {
            IConstWorkUnitInfo &wu = wus->query();
            ASSERT(streq(wu.queryClusterName(), "WuTestCluster0"));
            if (numIterated)
                ASSERT(strcmp(wu.queryStateDesc(), prevValue)<=0);
            prevValue.set(wu.queryStateDesc());
            numIterated++;
        }
        DBGLOG("%d workunits listed by cluster, descending state in %d ms", numIterated, msTick()-start);
        ASSERT(numIterated == (testSize+4)/5);

        // Check wild user
        WUSortField filterByWildUser[] = { (WUSortField) (WUSFuser|WUSFwild), WUSFterm };
        start = msTick();
        wus.setown(factory->getWorkUnitsSorted((WUSortField)(WUSFwuid|WUSFreverse), filterByWildUser, "WuTestUser0*", 0, 10000, NULL, NULL));
        numIterated = 0;
        ForEach(*wus)
        {
            IConstWorkUnitInfo &wu = wus->query();
            ASSERT(strncmp(wu.queryUser(), "WuTestUser0", 11)==0);
            if (numIterated)
                ASSERT(strcmp(wu.queryWuid(), prevValue)<0);
            prevValue.set(wu.queryWuid());
            numIterated++;
        }
        DBGLOG("%d workunits listed by wild user, descending wuid in %d ms", numIterated, msTick()-start);
        ASSERT(numIterated == (testSize/50)*10 + min(testSize % 50, 10));

        // Test sorted by totalThorTime, ascending
        start = msTick();
        numIterated = 0;
        unsigned prevThorTime = 0;
        wus.setown(factory->getWorkUnitsSorted((WUSortField)(WUSFtotalthortime), NULL, NULL, 0, 10000, NULL, NULL));
        ForEach(*wus)
        {
            IConstWorkUnitInfo &wu = wus->query();
            if (numIterated)
                ASSERT(wu.getTotalThorTime()>=prevThorTime);
            prevThorTime = wu.getTotalThorTime();
            numIterated++;
        }
        DBGLOG("%d workunits ascending thortime in %d ms", numIterated, msTick()-start);
        ASSERT_EQUAL(before, numIterated);

        // Test use of cache/page mechanism - on something needing a postsort
        start = msTick();
        __int64 cachehint = 0;
        numIterated = 0;
        int startRow = 0;
        for (;;)
        {
            wus.setown(factory->getWorkUnitsSorted((WUSortField)(WUSFstate|WUSFreverse), filterByCluster, "WuTestCluster0", startRow, 1, &cachehint, NULL));
            if (!wus->first())
                break;
            IConstWorkUnitInfo &wu = wus->query();
            ASSERT(streq(wu.queryClusterName(), "WuTestCluster0"));
            if (numIterated)
                ASSERT(strcmp(wu.queryStateDesc(), prevValue)<=0);
            prevValue.set(wu.queryStateDesc());
            numIterated++;
            bool nextSeen = wus->next();
            ASSERT(!nextSeen);
            startRow++;
        }
        DBGLOG("%d workunits filtered by cluster, descending state, page by page in %d ms", numIterated, msTick()-start);
        ASSERT(numIterated == (testSize+4)/5);

        // Test use of cache/page mechanism - on something NOT needing a postsort
        start = msTick();
        cachehint = 0;
        numIterated = 0;
        startRow = 0;
        for (;;)
        {
            wus.setown(factory->getWorkUnitsSorted((WUSortField)(WUSFwuid|WUSFreverse), filterByCluster, "WuTestCluster0", startRow, 1, &cachehint, NULL));
            if (!wus->first())
                break;
            IConstWorkUnitInfo &wu = wus->query();
            ASSERT(streq(wu.queryClusterName(), "WuTestCluster0"));
            if (numIterated)
                ASSERT(strcmp(wu.queryWuid(), prevValue)<0);
            prevValue.set(wu.queryWuid());
            numIterated++;
            bool nextSeen = wus->next();
            ASSERT(!nextSeen);
            startRow++;
        }
        DBGLOG("%d workunits filtered by cluster, descending wuid, page by page in %d ms", numIterated, msTick()-start);
        ASSERT(numIterated == (testSize+4)/5);

        // Test use of cache/page mechanism - on something NOT needing a postsort, ascending
        start = msTick();
        cachehint = 0;
        numIterated = 0;
        startRow = 0;
        for (;;)
        {
            wus.setown(factory->getWorkUnitsSorted((WUSortField)(WUSFwuid), filterByCluster, "WuTestCluster0", startRow, 1, &cachehint, NULL));
            if (!wus->first())
                break;
            IConstWorkUnitInfo &wu = wus->query();
            ASSERT(streq(wu.queryClusterName(), "WuTestCluster0"));
            if (numIterated)
                ASSERT(strcmp(wu.queryWuid(), prevValue)>0);
            prevValue.set(wu.queryWuid());
            numIterated++;
            bool nextSeen = wus->next();
            ASSERT(!nextSeen);
            startRow++;
        }
        DBGLOG("%d workunits filtered by cluster, ascending wuid, page by page in %d ms", numIterated, msTick()-start);
        ASSERT(numIterated == (testSize+4)/5);
    }

    void testList2()
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        bool isDali = streq(factory->queryStoreType(), "Dali");
        unsigned before = factory->numWorkUnits();
        unsigned start = msTick();
        unsigned numIterated = 0;
        Owned<IConstWorkUnitIterator> wus;
        // Test use of cache/page mechanism - sorted by totalThorTime, descending
        start = msTick();
        __int64 cachehint = 0;
        numIterated = 0;
        unsigned startRow = 0;
        unsigned prevThorTime = 0;
        for (;;)
        {
            wus.setown(factory->getWorkUnitsSorted((WUSortField)(WUSFtotalthortime|WUSFreverse), NULL, NULL, startRow, 1, &cachehint, NULL));
            if (!wus->first())
                break;
            IConstWorkUnitInfo &wu = wus->query();
            if (numIterated)
                ASSERT(wu.getTotalThorTime()<=prevThorTime);
            prevThorTime = wu.getTotalThorTime();
            numIterated++;
            bool nextSeen = wus->next();
            ASSERT(!nextSeen);
            wus.clear();
            startRow++;
        }
        DBGLOG("%d workunits descending thortime, page by page in %d ms", numIterated, msTick()-start);
        ASSERT_EQUAL(before, numIterated);
    }

    void testListByAppValue()
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        bool isDali = streq(factory->queryStoreType(), "Dali");
        unsigned start = msTick();
        unsigned numIterated = 0;
        // Test filter by appValue
        WUSortField filterByAppValue[] = { WUSFappvalue, WUSFterm };
        start = msTick();
        Owned<IConstWorkUnitIterator> wus = factory->getWorkUnitsSorted((WUSortField)(WUSFwuid|WUSFreverse), filterByAppValue, "appname/userId\0WuTestUser00", 0, 10000, NULL, NULL);
        ForEach(*wus)
        {
            IConstWorkUnitInfo &wu = wus->query();
            ASSERT(streq(wu.queryUser(), "WuTestUser00"));
            numIterated++;
        }
        DBGLOG("%d workunits by appvalue in %d ms", numIterated, msTick()-start);
        ASSERT(numIterated == (testSize+49)/50);
        numIterated++;
    }
    void testListByAppValueWild()
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        unsigned start = msTick();
        unsigned numIterated = 0;
        // Test filter by appValue
        WUSortField filterByAppValueWild[] = { (WUSortField) (WUSFappvalue|WUSFwild), WUSFterm };
        start = msTick();
        StringAttr prevValue;
        Owned<IConstWorkUnitIterator> wus = factory->getWorkUnitsSorted((WUSortField)(WUSFwuid|WUSFreverse), filterByAppValueWild, "appname/userId\0WuTestUser*", 0, 10000, NULL, NULL);
        ForEach(*wus)
        {
            IConstWorkUnitInfo &wu = wus->query();
            if (numIterated)
                ASSERT(strcmp(wu.queryWuid(), prevValue)<0);
            prevValue.set(wu.queryWuid());
            numIterated++;
        }
        DBGLOG("%d workunits by appvalue wild in %d ms", numIterated, msTick()-start);
        ASSERT(numIterated == testSize);
        numIterated++;
    }
    void testListByFilesRead()
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        unsigned start = msTick();
        unsigned numIterated = 0;
        // Test filter by filesRead
        WUSortField filterByFilesRead[] = { WUSFfileread, WUSFterm };
        StringAttr prevValue;
        Owned<IConstWorkUnitIterator> wus = factory->getWorkUnitsSorted((WUSortField)(WUSFwuid|WUSFreverse), filterByFilesRead, "myfile00", 0, 10000, NULL, NULL);
        ForEach(*wus)
        {
            IConstWorkUnitInfo &wu = wus->query();
            if (numIterated)
                ASSERT(strcmp(wu.queryWuid(), prevValue)<0);
            prevValue.set(wu.queryWuid());
            numIterated++;
        }
        DBGLOG("%d workunits by fileread wild in %d ms", numIterated, msTick()-start);
        ASSERT(numIterated == (testSize+9)/10);
        numIterated++;
    }
    void testListByFilesWritten()
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        unsigned start = msTick();
        unsigned numIterated = 0;
        // Test filter by filesRead
        WUSortField filterByFilesRead[] = { WUSFfilewritten, WUSFterm };
        StringAttr prevValue;
        Owned<IConstWorkUnitIterator> wus = factory->getWorkUnitsSorted((WUSortField)(WUSFwuid|WUSFreverse), filterByFilesRead, "myfilewritten00", 0, 10000, NULL, NULL);
        ForEach(*wus)
        {
            IConstWorkUnitInfo &wu = wus->query();
            if (numIterated)
                ASSERT(strcmp(wu.queryWuid(), prevValue)<0);
            prevValue.set(wu.queryWuid());
            numIterated++;
        }
        DBGLOG("%d workunits by filewritten wild in %d ms", numIterated, msTick()-start);
        ASSERT(numIterated == (testSize+9)/10);
        numIterated++;
    }
    void testSortByThorTime()
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        unsigned before = factory->numWorkUnits();
        unsigned start = msTick();
        unsigned numIterated = 0;
        // Test filter by filesRead
        WUSortField nofilter[] = { WUSFterm };
        unsigned prevValue = 0;
        Owned<IConstWorkUnitIterator> wus = factory->getWorkUnitsSorted((WUSortField)(WUSFtotalthortime+WUSFnumeric), nofilter, nullptr, 0, 10000, NULL, NULL);
        ForEach(*wus)
        {
            IConstWorkUnitInfo &wu = wus->query();
            if (numIterated)
                ASSERT(wu.getTotalThorTime() >= prevValue);
            prevValue=wu.getTotalThorTime();
            numIterated++;
        }
        DBGLOG("%d workunits by totalThorTime in %d ms", numIterated, msTick()-start);
        ASSERT(numIterated == before);
        numIterated++;
    }
    void testGlobal()
    {
        // Is global workunit ever actually used any more? For scalar persists, perhaps
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        StringAttr prevValue;
        Owned<IWorkUnit> global = factory->getGlobalWorkUnit(NULL, NULL);
        ASSERT(global != NULL);
        ASSERT(streq(global->queryWuid(), "global"));

        Owned<IWUResult> result = global->updateGlobalByName("Result 1");
        result->setResultScalar(true);
        result->setResultInt(53);
        global->commit();
        result.clear();
        global.clear();

        Owned<IConstWorkUnit> wu = factory->getGlobalWorkUnit(NULL, NULL);
        Owned<IConstWUResult> cresult = wu->getGlobalByName("Result 1");
        ASSERT(cresult);
        ASSERT(cresult->isResultScalar());
        ASSERT(cresult->getResultInt()==53);
    }
    void testWorkUnitServices()
    {
        class DummyContext: implements ICodeContext
        {
            virtual const char *loadResource(unsigned id) { throwUnexpected(); }

            // Fetching interim results from workunit/query context

            virtual bool getResultBool(const char * name, unsigned sequence) { throwUnexpected(); }
            virtual void getResultData(unsigned & tlen, void * & tgt, const char * name, unsigned sequence) { throwUnexpected(); }
            virtual void getResultDecimal(unsigned tlen, int precision, bool isSigned, void * tgt, const char * stepname, unsigned sequence) { throwUnexpected(); }
            virtual void getResultDictionary(size32_t & tcount, const byte * * & tgt, IEngineRowAllocator * _rowAllocator, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer, IHThorHashLookupInfo * hasher) override { throwUnexpected(); }
            virtual void getResultRaw(unsigned & tlen, void * & tgt, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) { throwUnexpected(); }
            virtual void getResultSet(bool & isAll, size32_t & tlen, void * & tgt, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) { throwUnexpected(); }
            virtual __int64 getResultInt(const char * name, unsigned sequence) { throwUnexpected(); }
            virtual double getResultReal(const char * name, unsigned sequence) { throwUnexpected(); }
            virtual void getResultRowset(size32_t & tcount, const byte * * & tgt, const char * name, unsigned sequence, IEngineRowAllocator * _rowAllocator, bool isGrouped, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) override { throwUnexpected(); }
            virtual void getResultString(unsigned & tlen, char * & tgt, const char * name, unsigned sequence) { throwUnexpected(); }
            virtual void getResultStringF(unsigned tlen, char * tgt, const char * name, unsigned sequence) { throwUnexpected(); }
            virtual void getResultUnicode(unsigned & tlen, UChar * & tgt, const char * name, unsigned sequence) { throwUnexpected(); }
            virtual char *getResultVarString(const char * name, unsigned sequence) { throwUnexpected(); }
            virtual UChar *getResultVarUnicode(const char * name, unsigned sequence) { throwUnexpected(); }

            // Writing results to workunit/query context/output

            virtual void setResultBool(const char *name, unsigned sequence, bool value) { throwUnexpected(); }
            virtual void setResultData(const char *name, unsigned sequence, int len, const void * data) { throwUnexpected(); }
            virtual void setResultDecimal(const char * stepname, unsigned sequence, int len, int precision, bool isSigned, const void *val) { throwUnexpected(); }
            virtual void setResultInt(const char *name, unsigned sequence, __int64 value, unsigned size) { throwUnexpected(); }
            virtual void setResultRaw(const char *name, unsigned sequence, int len, const void * data) { throwUnexpected(); }
            virtual void setResultReal(const char * stepname, unsigned sequence, double value) { throwUnexpected(); }
            virtual void setResultSet(const char *name, unsigned sequence, bool isAll, size32_t len, const void * data, ISetToXmlTransformer * transformer) { throwUnexpected(); }
            virtual void setResultString(const char *name, unsigned sequence, int len, const char * str) { throwUnexpected(); }
            virtual void setResultUInt(const char *name, unsigned sequence, unsigned __int64 value, unsigned size) { throwUnexpected(); }
            virtual void setResultUnicode(const char *name, unsigned sequence, int len, UChar const * str) { throwUnexpected(); }
            virtual void setResultVarString(const char * name, unsigned sequence, const char * value) { throwUnexpected(); }
            virtual void setResultVarUnicode(const char * name, unsigned sequence, UChar const * value) { throwUnexpected(); }

            // Checking persists etc are up to date

            virtual unsigned getResultHash(const char * name, unsigned sequence) { throwUnexpected(); }
            virtual unsigned getExternalResultHash(const char * wuid, const char * name, unsigned sequence) { throwUnexpected(); }
            virtual unsigned __int64 getDatasetHash(const char * name, unsigned __int64 crc) { throwUnexpected(); }

            // Fetching various environment information, typically accessed via std.system

            virtual char *getClusterName() { throwUnexpected(); } // caller frees return string.
            virtual char *getEnv(const char *name, const char *defaultValue) const { throwUnexpected(); }
            virtual char *getGroupName() { throwUnexpected(); } // caller frees return string.
            virtual char *getJobName() { throwUnexpected(); } // caller frees return string.
            virtual char *getJobOwner() { throwUnexpected(); } // caller frees return string.
            virtual unsigned getNodeNum() { throwUnexpected(); }
            virtual unsigned getNodes() { throwUnexpected(); }
            virtual char *getOS() { throwUnexpected(); } // caller frees return string
            virtual char *getPlatform() { throwUnexpected(); } // caller frees return string.
            virtual unsigned getPriority() const { throwUnexpected(); }
            virtual char *getWuid() { throwUnexpected(); } // caller frees return string.

            // Exception handling

            virtual void addWuException(const char*, unsigned int, unsigned int, const char*) { throwUnexpected(); } //n.b. this might be better named: it should only be used for adding user-generated exceptions (via the logging plug-in) --- there's a call in IAgentContext which takes a source argument too
            virtual void addWuAssertFailure(unsigned code, const char * text, const char * filename, unsigned lineno, unsigned column, bool isAbort) { throwUnexpected(); }

            // File resolution etc

            virtual char * getExpandLogicalName(const char * logicalName) { throwUnexpected(); }
            virtual unsigned __int64 getFileOffset(const char *logicalPart) { throwUnexpected(); }
            virtual char *getFilePart(const char *logicalPart, bool create=false) { throwUnexpected(); } // caller frees return string.
            virtual IDistributedFileTransaction *querySuperFileTransaction() { throwUnexpected(); }
            virtual IUserDescriptor *queryUserDescriptor() { throwUnexpected(); }

            // Graphs, child queries etc

            virtual void executeGraph(const char * graphName, bool realThor, size32_t parentExtractSize, const void * parentExtract) { throwUnexpected(); }
            virtual unsigned getGraphLoopCounter() const { return 0; }
            virtual IThorChildGraph * resolveChildQuery(__int64 activityId, IHThorArg * colocal) { throwUnexpected(); }
            virtual IEclGraphResults * resolveLocalQuery(__int64 activityId) { return NULL; }

            // Logging etc

            virtual unsigned logString(const char *text) const { throwUnexpected(); }
            virtual IDebuggableContext *queryDebugContext() const { return NULL; }

            // Memory management

            virtual IEngineRowAllocator * getRowAllocator(IOutputMetaData * meta, unsigned activityId) const { throwUnexpected(); }
            virtual IEngineRowAllocator * getRowAllocatorEx(IOutputMetaData * meta, unsigned activityId, unsigned heapFlags) const { throwUnexpected(); }
            virtual const char * cloneVString(const char *str) const { throwUnexpected(); }
            virtual const char * cloneVString(size32_t len, const char *str) const { throwUnexpected(); }

            // Called from generated code for FROMXML/TOXML

            virtual const void * fromXml(IEngineRowAllocator * _rowAllocator, size32_t len, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace) { throwUnexpected(); }
            virtual void getRowXML(size32_t & lenResult, char * & result, IOutputMetaData & info, const void * row, unsigned flags) { throwUnexpected(); }

            // Miscellaneous

            virtual void getExternalResultRaw(unsigned & tlen, void * & tgt, const char * wuid, const char * stepname, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) { throwUnexpected(); }    // shouldn't really be here, but it broke thor.
            virtual char * queryIndexMetaData(char const * lfn, char const * xpath) { throwUnexpected(); }

            // Called from generated code for FROMJSON

            virtual const void * fromJson(IEngineRowAllocator * _rowAllocator, size32_t len, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace) { throwUnexpected(); }
            virtual void getRowJSON(size32_t & lenResult, char * & result, IOutputMetaData & info, const void * row, unsigned flags) { throwUnexpected(); }

            virtual const IContextLogger &queryContextLogger() const
            {
                return queryDummyContextLogger();
            }
            virtual IEngineContext *queryEngineContext() { return NULL; }
            virtual char *getDaliServers() { throwUnexpected(); }
            virtual IWorkUnit* updateWorkUnit() const { throwUnexpected(); }
            virtual ISectionTimer * registerTimer(unsigned activityId, const char * name) { throwUnexpected(); }
        } ctx;

 #if 0
        // These tests are not valid at present, since the C++ structure does not accurately describe the variable layout fields
        size32_t lenResult;
        void * result;
        wsWorkunitList(&ctx, lenResult, result, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, true, false, NULL);
        /* export WsWorkunitRecord := record "
                                    " string24 wuid;"                         0
                                    " string owner{maxlength(64)};"           24
                                    " string cluster{maxlength(64)};"         92
                                    " string roxiecluster{maxlength(64)};"    160
                                    " string job{maxlength(256)};"            228
                                    " string10 state;"                        488
                                    " string7 priority;"                      498
                                    " integer2 priorityvalue;"                505
                                    " string20 created;"                      507
                                    " string20 modified;"                     527
                                    " boolean online;"                        547
                                    " boolean protected;"                     548
                                  " end;\n"  Total size 549
                                  */
        #pragma pack(push, 1)
        struct resultStruct
        {
            char wuid[24];   // space filled
            unsigned _len_owner; char owner[64];
            unsigned _len_cluster; char cluster[64];
            unsigned _len_roxiecluster; char roxiecluster[64];
            unsigned _len_job; char job[256];
            char state[10];
            char priority[7];
            unsigned short priorityValue;
            char created[20];
            char modified[20];
            bool online;
            bool isProtected;
        };
        #pragma pack(pop)
        ASSERT(lenResult == testSize * sizeof(resultStruct));
        rtlFree(result);

        // Now filter owner via generic mechanism
        unsigned start = msTick();
        wsWorkunitList(&ctx, lenResult, result, NULL, NULL, "WuTestUser00", NULL, NULL, "completed", NULL, NULL, NULL, NULL, NULL, true, false, NULL);
        ASSERT(lenResult % sizeof(resultStruct) == 0);
        unsigned numResults = lenResult/sizeof(resultStruct);
        resultStruct *it = (resultStruct *) result;
        for (unsigned i = 0; i < numResults; i++)
        {
            ASSERT(memicmp(it[i].state, "completed ", 10)==0);
            ASSERT(memicmp(it[i].owner, "WuTestUser00       ", 20)==0);
        }
        DBGLOG("%d owned workunits listed the hard way in %d ms", numResults, msTick()-start);
        ASSERT(numResults <= (testSize+49)/50);  // Not sure what the exact answer should be!

        rtlFree(result);
#endif
    }
};
StringArray WuTool::wuids;

CPPUNIT_TEST_SUITE_REGISTRATION( WuTool );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( WuTool, "WuTool" );

class WuDetails : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(WuDetails);
        CPPUNIT_TEST(testWuDetails);
    CPPUNIT_TEST_SUITE_END();
protected:
    class AttributeScopeVisitor : public IWuScopeVisitor
    {
    public:
        virtual void noteStatistic(StatisticKind kind, unsigned __int64 value, IConstWUStatistic & cur) override
        {
            StringBuffer text;
            text.append(value);
            noteProperty(queryStatisticName(kind), text);
        }
        virtual void noteAttribute(WuAttr attr, const char * value)
        {
            noteProperty(queryWuAttributeName(attr), value);
        }
        virtual void noteHint(const char * kind, const char * value)
        {
            noteProperty(kind, value);
        }
        virtual void noteProperty(const char * kind, const char * value)
        {
            DBGLOG("  Attr %s=%s", kind, value);
        }
    };

    void testWuDetails(IConstWorkUnit * wu)
    {
        const WuScopeFilter filter{};
        Owned<IConstWUScopeIterator> iter = &wu->getScopeIterator(filter);
        DBGLOG("%s %s", wu->queryWuid(), wu->queryClusterName());
        AttributeScopeVisitor visitor;
        StringBuffer prevScope;
        ForEach(*iter)
        {
            const char * scope = iter->queryScope();
            DBGLOG("Scope: %s %s", scope, queryScopeTypeName(iter->getScopeType()));
            //Ensure the scopes are iterated in the correct order.
            if (!prevScope.isEmpty())
                ASSERT(compareScopeName(prevScope.str(), scope) < 0);
            prevScope.set(scope);

            iter->playProperties(visitor);
        }
    }

    void testWuDetails()
    {
        /*
         * The following are the tests that need to be done on the filters
         * o Source
         * o Scope type
         * o Set of scope types (activity, edges)
         * o Depth
         * o Single scope
         * o Set of scopes
         * o Set of ids
         * They should all be selectable via a text filter.
         */
#if 0
        WUSortField filterByJob[] = { WUSFjob, WUSFterm };
        CCycleTimer timer;
        Owned<IConstWorkUnitIterator> wus;
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        wus.setown(factory->getWorkUnitsSorted((WUSortField) (WUSFwuid | WUSFreverse), filterByJob, "sqfilt-multiPart(false)*\0", 0, 10000, NULL, NULL));
        ForEach(*wus)
        {
            IConstWorkUnitInfo &wuinfo = wus->query();
            Owned<IConstWorkUnit> wu = factory->openWorkUnit(wuinfo.queryWuid());
            testWuDetails(wu);
        }
        wus.clear();

        DBGLOG("wuDetails %u ms", timer.elapsedMs());
#endif
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( WuDetails );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( WuDetails, "WuDetails" );

#endif
