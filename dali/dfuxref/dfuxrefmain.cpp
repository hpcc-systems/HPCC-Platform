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

// DFU XREF Program

#include "jlib.hpp"
#include "jmisc.hpp"
#include "jptree.hpp"
#include "dalienv.hpp"
#include "daclient.hpp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/dali/dfuxref/dfuxrefmain.cpp $ $Id: dfuxrefmain.cpp 62561 2011-02-16 13:59:53Z nhicks $");

static bool AddCompleteOrphans = false;
static bool DeleteEmptyFiles = false;
static bool fixSizes = false;
static bool fixGroups = false;
static bool verbose = false;

#include "dfuxreflib.hpp"



#ifdef MAIN_DFUXREF

#if 1
void usage(const char *progname)
{
    printf("usage DFUXREF <dali-server> /c:<cluster-name> /d:<dir-name> [ /backupcheck ] \n");
    printf("or    DFUXREF <dali-server> /e:<cluster-name>  -- updates ECLwatch information\n");
}

int main(int argc, char* argv[])
{
    InitModuleObjects();
    EnableSEHtoExceptionMapping();
    setNodeCaching(true);
    StringBuffer logName("dfuxref");
    StringBuffer aliasLogName(logName);
    aliasLogName.append(".log");

    ILogMsgHandler *fileMsgHandler = getRollingFileLogMsgHandler(logName.str(), ".log", MSGFIELD_STANDARD, false, true, NULL, aliasLogName.str());
    queryLogMsgManager()->addMonitorOwn(fileMsgHandler, getCategoryLogMsgFilter(MSGAUD_all, MSGCLS_all, TopDetail));
    queryStderrLogMsgHandler()->setMessageFields(MSGFIELD_prefix);
    StringBuffer cmdline;
    unsigned i;
    for (i=0;i<(unsigned)argc;i++)
      cmdline.append(' ').append(argv[i]);

    if (argc<3) {
        usage(argv[0]);
        return 0;
    }

    SocketEndpoint ep;
    SocketEndpointArray epa;
    ep.set(argv[1],DALI_SERVER_PORT);
    epa.append(ep);
    Owned<IGroup> group = createIGroup(epa); 
    try {
        initClientProcess(group,DCR_Dfu);
        setPasswordsFromSDS();
        unsigned ndirs = 0;
        unsigned nclusters = 0;
        const char **dirs = (const char **)malloc(argc*sizeof(const char *));
        const char **clusters = (const char **)malloc(argc*sizeof(const char *));
        bool backupcheck = false;
        unsigned mode = PMtextoutput|PMcsvoutput|PMtreeoutput;
        for (i=2;i<(unsigned)argc;i++) {
            if (argv[i][0]=='/') {
                if (stricmp(argv[i],"/backupcheck")==0) {
                    mode = PMbackupoutput;
                }
                else  {
                    const char *arg="";
                    unsigned ni=i;
                    if (argv[i][1]&&(argv[i][2]==':')) {
                        arg = argv[i]+3;
                    }
                    else  if ((i+1<(unsigned)argc)&&argv[i+1][0]) {
                        ni++;
                        arg = argv[ni];
                    }
                    switch (toupper(argv[i][1])) {
                    case 'E':
                        // fall through
                        mode = PMtextoutput|PMupdateeclwatch;
                    case 'C':
                        clusters[nclusters++] = arg;
                        break;
                    case 'D':
                        dirs[ndirs++] = arg;
                        break;
                    default:
                        ni = argc;
                        nclusters = 0;
                    }
                    i = ni;
                }
            }
        }

        if (nclusters==0)
            usage(argv[0]);
        else {
            DBGLOG("Starting%s",cmdline.str());
            IPropertyTree * pReturnTree = RunProcess(nclusters,clusters,ndirs,dirs,mode,NULL,4);
            if (pReturnTree) {
                saveXML("dfutree.xml",pReturnTree);
            }
        }
    }
    catch (IException *e) {
        pexception("Exception",e);
        e->Release();
    }

    DBGLOG("Finished%s",cmdline.str());
    closeEnvironment();
    closedownClientProcess();

    releaseAtoms();
    return 0;
}

#else // testing runXRef


class Ccallback: public CInterface, implements IXRefProgressCallback
{
public: 
    IMPLEMENT_IINTERFACE;
    virtual void progress(const char *text)
    {
        printf("PROGRESS: %s\n",text);
    }
    virtual void error(const char *text)
    {
        printf("ERROR: %s\n",text);
    }
    
};

int main(int argc, char* argv[])
{
    InitModuleObjects();
    setNodeCaching(true);
    SocketEndpoint ep;
    SocketEndpointArray epa;
    ep.set(argv[1],DALI_SERVER_PORT);
    epa.append(ep);
    Owned<IGroup> group = createIGroup(epa); 
    try {
        initClientProcess(group);
        setPasswordsFromSDS();

        const char *clusters[2];
        clusters[0] = "thor_data50";
        clusters[1] = "thor_data100";
        Owned <Ccallback> callback = new Ccallback;
        Owned<IPropertyTree> tree = runXRef(2,clusters,callback,4);
        if (tree) { // succeeded
            printf("==============================================================\n");
            StringBuffer res;
            toXML(tree, res, 2);
            fwrite(res.str(),res.length(),1,stdout);
        }

    }
    catch (IException *e) {
        pexception("Exception",e);
        e->Release();
    }

    closeEnvironment();
    closedownClientProcess();

    releaseAtoms();
    return 0;
}

#endif
#endif
