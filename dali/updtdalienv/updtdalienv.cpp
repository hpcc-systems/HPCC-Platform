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

#include "platform.h"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "jptree.hpp"
#include "mpbase.hpp"
#include "mpcomm.hpp"
#include "jarray.hpp"
#include "jencrypt.hpp"
#include "jregexp.hpp"

#include "daclient.hpp"
#include "dasds.hpp"
#include "dadfs.hpp"
#include "dautils.hpp"
#include "jexcept.hpp"

#ifdef _WIN32
#include <conio.h>
#else
#define _getch getchar
#define _putch putchar
#endif

static void usage(const char *exe)
{
    printf("Update dali with environment.xml changes:\n");
    printf("  %s <environment-xml-file> [-i <dali-ip>] [-f]\n", exe);
    printf("Retrieve directory information:\n"); 
    printf("  %s <environment-xml-file> -d category component instance [-ip ip]\n", exe);
}


int main(int argc, char* argv[])
{

    InitModuleObjects();
    EnableSEHtoExceptionMapping();
    if (argc<2) {
        usage(argv[0]);
        return -1;
    }
    bool forceGroupUpdate = false;
    StringBuffer filename;
    StringBuffer inst;
    StringBuffer dcat;
    StringBuffer dcomp;
    StringBuffer dinst;
    StringBuffer dip;
    for (int i=1;i<argc;i++) {
        if (argv[i][0]=='-') {
            if ((stricmp(argv[i],"-i")==0)&&(i+1<argc)) {
                inst.append(argv[++i]);
            }
            else if (0==stricmp(argv[i],"-f")) {
                forceGroupUpdate = true;
            }
            else if ((stricmp(argv[i],"-d")==0)&&(i+3<argc)) {
                dcat.append(argv[++i]);
                dcomp.append(argv[++i]);
                dinst.append(argv[++i]);
            }
            else if ((stricmp(argv[i],"-ip")==0)&&(i+1<argc)) {
                dip.append(argv[++i]);
            }
            else {
                usage(argv[0]);
                return -1;
            }
        }
        else {
            if (filename.length()) {
                usage(argv[0]);
                return -1;
            }
            filename.append(argv[i]);
        }
    }       

    Owned<IPropertyTree> env;
    try {
        env.setown(createPTreeFromXMLFile(argv[1]));
        if (!env.get()) {
            fprintf(stderr,"Could not load Environment from %s\n",argv[1]);
            return 1;
        }
        const char *s = env->queryName();
        if (!s||(strcmp(s,"Environment")!=0)) {
            fprintf(stderr,"File %s is invalid\n",argv[1]);
            return 1;
        }
    }
    catch (IException *e) {
        StringBuffer err;
        e->errorMessage(err);
        fprintf(stderr,"Could not load Environment from %s, %s\n",argv[1],err.str());
        return 1;
    }
    int ret = 0;
    try {
        if (dcat.length()) {
            IPropertyTree* dirs = env->queryPropTree("Software/Directories");
            StringBuffer dirout;
            if (getConfigurationDirectory(dirs,dcat.str(),dcomp.str(),dinst.str(),dirout)&&dirout.length()) {
                if (dip.length()) {
                    SocketEndpoint ep(dip.str());
                    RemoteFilename rfn;
                    rfn.setPath(ep,dirout.str());
                    rfn.getRemotePath(dirout.clear());
                }
                printf("%s",dirout.str());
            }
            else {
                ret = 1;
            }
        }
        else {
            if (!updateDaliEnv(env, forceGroupUpdate, inst.str()))
                ret = 1;
        }
    }
    catch (IException *e) {
        pexception("updtdalienv",e);
        e->Release();
        ret = 1;
    }
    releaseAtoms();
    return ret;
}

