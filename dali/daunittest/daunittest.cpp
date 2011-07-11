
#include "platform.h"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "mpbase.hpp"/*##############################################################################

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


#include "mpcomm.hpp"
#include "sockfile.hpp"

#include "daclient.hpp"
#include "dadfs.hpp"
#include "dafdesc.hpp"
#include "dasds.hpp"
#include "danqs.hpp"
#include "dautils.hpp"
#include "mplog.hpp"


#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/TestResult.h>
#include <cppunit/TestFailure.h>
#include <cppunit/BriefTestProgressListener.h>
#include <cppunit/TestResultCollector.h>




static void usage(const char *error=NULL)
{
    if (error) 
        printf("ERROR: %s\n", error);
    printf("usage: daunittest <dali-ip>"); 
}


class DfsTestProgressListener : public CPPUNIT_NS::TestListener
{
public:
    DfsTestProgressListener() : m_lastTestFailed( false ) {}
    virtual ~DfsTestProgressListener() {}
    
    void startTest( CPPUNIT_NS::Test *test )
    {
        PROGLOG("TEST(%s): START",test->getName().c_str());
        m_lastTestFailed = false;
    }
    
    void addFailure( const CPPUNIT_NS::TestFailure &failure )
    {
        StringBuffer s;
        s.appendf("TEST(%s): %s File: %s Ln:%d",failure.failedTestName().c_str(),failure.isError()?"":"ASSERT ",(const char *)failure.sourceLine().fileName().c_str(),(unsigned)failure.sourceLine().lineNumber());
        CPPUNIT_NS::Exception *e = failure.thrownException();
        if (e)
            s.appendf(" %s %s",e->message().shortDescription().c_str(),e->message().details().c_str());
        ERRLOG("%s",s.str());
        m_lastTestFailed  = true;
    }
    
    void endTest( CPPUNIT_NS::Test *test )
    {
        PROGLOG("TEST(%s): END%s",test->getName().c_str(),m_lastTestFailed?"":" OK");
    }
    
private:
    /// Prevents the use of the copy constructor.
    DfsTestProgressListener( const DfsTestProgressListener &copy );
    
    /// Prevents the use of the copy operator.
    void operator =( const DfsTestProgressListener &copy );
    
private:
    bool m_lastTestFailed;
};


int main( int argc, char **argv )
{

    enableMemLeakChecking(true);
    struct ReleaseAtomBlock { ~ReleaseAtomBlock() { releaseAtoms(); } } rABlock;
    InitModuleObjects();
    EnableSEHtoExceptionMapping();
    int ret=1;

    try {
        StringBuffer cmd;
        splitFilename(argv[0], NULL, NULL, &cmd, NULL);
        openLogFile(cmd.toLowerCase().append(".log").str());
        if (argc<2) {
            usage();
            return 1;
        }

        SocketEndpoint ep;
        ep.set(argv[1],DALI_SERVER_PORT);
        if (ep.isNull()) {
            usage("could not resolve dali server IP");
            return 1;
        }
        SocketEndpointArray epa;
        epa.append(ep);
        Owned<IGroup> group = createIGroup(epa); 
        initClientProcess(group, DCR_Other);


        // Create the event manager and test controller
        CPPUNIT_NS::TestResult controller;

        // Add a listener that colllects test result
        CPPUNIT_NS::TestResultCollector result;
        controller.addListener( &result );        

        // Add a listener that print dots as test run.
        //CPPUNIT_NS::BriefTestProgressListener progress;
        DfsTestProgressListener progress;
        controller.addListener( &progress );      

        // Add the top suite to the test runner
        CPPUNIT_NS::TestRunner runner;
        runner.addTest( CPPUNIT_NS::TestFactoryRegistry::getRegistry().makeTest() );
        runner.run( controller );

        ret = result.wasSuccessful() ? 0 : 1;

        closedownClientProcess();
        setNodeCaching(false);
    }
    catch (IException *e) {
        EXCLOG(e,"daunittest Exception");
    }
    return ret;
}
