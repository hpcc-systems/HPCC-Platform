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

// DFU XREF Program

#include "jlib.hpp"
#include "jmisc.hpp"
#include "jptree.hpp"
#include "dalienv.hpp"
#include "daclient.hpp"

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

    {
        Owned<IComponentLogFileCreator> lf = createComponentLogFileCreator("dfuxref");
        lf->setAppend(false);
        lf->setMaxDetail(TopDetail);
        lf->setMsgFields(MSGFIELD_STANDARD);
        lf->beginLogging();
    }
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
