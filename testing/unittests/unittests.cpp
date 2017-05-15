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

#ifdef _USE_CPPUNIT
#include "unittests.hpp"
#include "jstats.h"
#include "jregexp.hpp"
#include "jfile.hpp"
#include "deftype.hpp"
#include "rmtfile.hpp"

/*
 * This is the main unittest driver for HPCC. From here,
 * all unit tests, be they internal or external (API),
 * will run.
 *
 * All internal unit tests, written on the same source
 * files as the implementation they're testing, can be
 * dynamically linked via the helper class below.
 *
 * All external unit tests (API tests, test-driven
 * development, interface documentation and general
 * usability tests) should be implemented as source
 * files within the same directory as this file, and
 * statically linked together.
 *
 * CPPUnit will automatically recognise and run them all.
 */

void usage()
{
    printf("\n"
    "Usage:\n"
    "    unittests <options> <testnames>\n"
    "\n"
    "Options:\n"
    "    -a  --all        Include all tests, including timing and stress tests\n"
    "    -d  --load path  Dynamically load a library/all libraries in a directory.\n"
    "                     By default, the HPCCSystems lib directory is loaded.\n"
    "    -e  --exact      Match subsequent test names exactly\n"
    "    -h  --help       Display this help text\n"
    "    -l  --list       List matching tests but do not execute them\n"
    "    -x  --exclude    Exclude subsequent test names\n"
    "\n");
}

bool matchName(const char *name, const StringArray &patterns)
{
    ForEachItemIn(idx, patterns)
    {
        bool match;
        const char *pattern = patterns.item(idx);
        if (strchr(pattern, '*'))
        {
            match = WildMatch(name, pattern, true);
        }
        else
            match = streq(name, pattern);
        if (match)
            return true;
    }
    return false;
}

LoadedObject *loadDll(const char *thisDll)
{
    try
    {
        DBGLOG("Loading %s", thisDll);
        return new LoadedObject(thisDll);
    }
    catch (IException *E)
    {
        E->Release();
    }
    catch (...)
    {
    }
    return NULL;
}

void loadDlls(IArray &objects, const char * libDirectory)
{
    const char * mask = "*" SharedObjectExtension;
    Owned<IFile> libDir = createIFile(libDirectory);
    Owned<IDirectoryIterator> libFiles = libDir->directoryFiles(mask,false,false);
    ForEach(*libFiles)
    {
        const char *thisDll = libFiles->query().queryFilename();
        if (!strstr(thisDll, "javaembed"))  // Bit of a hack, but loading this if java not present terminates...
            if (!strstr(thisDll, "py2embed"))      // These two clash, so ...
                if (!strstr(thisDll, "py3embed"))  // ... best to load neither...
                {
                    LoadedObject *loaded = loadDll(thisDll);
                    if (loaded)
                        objects.append(*loaded);
                }
    }
}

int main(int argc, char* argv[])
{
    InitModuleObjects();

    StringArray includeNames;
    StringArray excludeNames;
    StringArray loadLocations;
    bool wildMatch = true;
    bool exclude = false;
    bool includeAll = false;
    bool verbose = false;
    bool list = false;
    bool useDefaultLocations = true;
    for (int argNo = 1; argNo < argc; argNo++)
    {
        const char *arg = argv[argNo];
        if (arg[0]=='-')
        {
            if (streq(arg, "-x") || streq(arg, "--exclude"))
                exclude = true;
            else if (streq(arg, "-v") || streq(arg, "--verbose"))
                verbose = true;
            else if (streq(arg, "-e") || streq(arg, "--exact"))
                wildMatch = false;
            else if (streq(arg, "-a") || streq(arg, "--all"))
                includeAll = true;
            else if (streq(arg, "-l") || streq(arg, "--list"))
                list = true;
            else if (streq(arg, "-d") || streq(arg, "--load"))
            {
                useDefaultLocations = false;
                argNo++;
                if (argNo<argc)
                   loadLocations.append(argv[argNo]);
            }
            else
            {
                usage();
                exit(streq(arg, "-h") || streq(arg, "--help")?0:4);
            }
        }
        else
        {
            VStringBuffer pattern("*%s*", arg);
            if (wildMatch && !strchr(arg, '*'))
                arg = pattern.str();
            if (exclude)
                excludeNames.append(arg);
            else
                includeNames.append(arg);
        }
    }
    if (verbose)
        queryStderrLogMsgHandler()->setMessageFields(MSGFIELD_time);
    else
        removeLog();

    if (!includeNames.length())
        includeNames.append("*");

    if (!includeAll)
    {
        excludeNames.append("*stress*");
        excludeNames.append("*timing*");
    }

    if (useDefaultLocations)
    {
        // Default library location depends on the executable location...
        StringBuffer dir;
        splitFilename(argv[0], &dir, &dir, NULL, NULL);
        dir.replaceString(PATHSEPSTR "bin" PATHSEPSTR, PATHSEPSTR "lib" PATHSEPSTR);
        if (verbose)
            DBGLOG("Adding default library location %s", dir.str());
        loadLocations.append(dir);
#ifdef _DEBUG
        dir.replaceString(PATHSEPSTR "lib" PATHSEPSTR, PATHSEPSTR "libs" PATHSEPSTR);
        loadLocations.append(dir);
        if (verbose)
            DBGLOG("Adding default library location %s", dir.str());
#endif
    }
    IArray objects;
    ForEachItemIn(idx, loadLocations)
    {
        const char *location = loadLocations.item(idx);
        Owned<IFile> file = createIFile(location);
        switch (file->isDirectory())
        {
        case notFound:
            if (verbose && !useDefaultLocations)
                DBGLOG("Specified library location %s not found", location);
            break;
        case foundYes:
            loadDlls(objects, location);
            break;
        case foundNo:
            LoadedObject *loaded = loadDll(location);
            if (loaded)
                objects.append(*loaded);
            break;
        }
    }

    bool wasSuccessful = false;
    {
        // New scope as we need the TestRunner to be destroyed before unloading the dlls...
        CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
        CppUnit::TextUi::TestRunner runner;
        CppUnit::Test *all = registry.makeTest();
        int numTests = all->getChildTestCount();
        for (int i = 0; i < numTests; i++)
        {
            CppUnit::Test *sub = all->getChildTestAt(i);
            std::string name = sub->getName();
            if (matchName(name.c_str(), includeNames))
            {
                if (matchName(name.c_str(), excludeNames))
                {
                    if (verbose)
                        DBGLOG("Excluding test %s", name.c_str());
                }
                else if (list)
                    printf("%s\n", name.c_str());
                else
                {
                    if (verbose)
                        DBGLOG("Including test %s", name.c_str());
                    runner.addTest(sub);
                }
            }
        }
        wasSuccessful = list || runner.run( "", false );
    }
    releaseAtoms();
    ClearTypeCache();   // Clear this cache before the file hooks are unloaded
    removeFileHooks();

    objects.kill();
    ExitModuleObjects();
    return wasSuccessful;
}


//MORE: This can't be included in jlib because of the dll dependency
class InternalStatisticsTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( InternalStatisticsTest  );
        CPPUNIT_TEST(testMappings);
    CPPUNIT_TEST_SUITE_END();

    void testMappings()
    {
        try
        {
            verifyStatisticFunctions();
        }
        catch (IException * e)
        {
            StringBuffer msg;
            fprintf(stderr, "Failure: %s", e->errorMessage(msg).str());
            e->Release();
            ASSERT(false);
        }
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( InternalStatisticsTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( InternalStatisticsTest, "StatisticsTest" );

class PtreeThreadingTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( PtreeThreadingTest  );
        CPPUNIT_TEST(testContention);
    CPPUNIT_TEST_SUITE_END();

    void testContention()
    {
        _testContention(ipt_lowmem);
        _testContention(ipt_fast);
    }
    void _testContention(byte flags)
    {
        enum ContentionMode { max_contention, some_contention, min_contention, some_control, min_control };
        class casyncfor: public CAsyncFor
        {
            volatile int v;
            void donothing()
            {
                v++;
            }
            byte flags = ipt_none;
            ContentionMode mode = max_contention;
            int iterations = 0;
            const char *desc = nullptr;

        public:
            casyncfor(const char *_desc, byte _flags, ContentionMode _mode, int _iter)
            : flags(_flags), mode(_mode), iterations(_iter), desc(_desc)
            {
            };
            double For(unsigned num, unsigned maxatonce, double overhead = 0.0)
            {
                unsigned start = msTick();
                CAsyncFor::For(num, maxatonce);
                unsigned elapsed = msTick()-start;
                double looptime = (elapsed * 1.0) / (iterations*num);
                if (mode < 3)
                    DBGLOG("%s (%s) test completed in %u ms (%f ms/iter)", desc, flags & ipt_fast ? "fast" : "lowmem", elapsed, looptime-overhead);
                return looptime;
            }
            void Do(unsigned i)
            {
                for (unsigned i = 0; i < iterations; i++)
                {
                    Owned<IPropertyTree> p = mode >= some_control ? nullptr : createPTreeFromXMLString(
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
                    , flags);
                    switch(mode)
                    {
                    case some_contention: case some_control: for (int j = 0; j < 100000; j++) donothing(); break;
                    case min_contention: case min_control: for (int j = 0; j < 1000000; j++) donothing(); break;
                    }
                }
            }
        } max("maxContention",flags,max_contention,1000),
          some("someContention",flags,some_contention,200),
          min("minContention",flags,min_contention,200),
          csome("control some",flags,some_control,200),
          cmin("control min",flags,min_control,200),
          seq("single",flags,max_contention,1000);
        max.For(8,8);
        some.For(8,8,csome.For(8,8));
        min.For(8,8,cmin.For(8,8));
        seq.For(8,1);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( PtreeThreadingTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( PtreeThreadingTest, "PtreeThreadingTest" );

//MORE: This can't be included in jlib because of the dll dependency
class StringBufferTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( StringBufferTest  );
        CPPUNIT_TEST(testReplace);
    CPPUNIT_TEST_SUITE_END();

    void testReplace()
    {
        StringBuffer r ("1 bb c");
        r.replaceString(" ", "x");
        ASSERT(streq(r, "1xbbxc"));
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( StringBufferTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( StringBufferTest, "StringBufferTest" );


#endif // _USE_CPPUNIT
