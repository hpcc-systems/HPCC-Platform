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

