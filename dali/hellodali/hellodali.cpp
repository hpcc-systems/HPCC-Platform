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
#include "jptree.hpp"
#include "jexcept.hpp"

#include "mpbase.hpp"
#include "mpcomm.hpp"

#include "daclient.hpp"
#include "dasds.hpp"


void usage(const char *exe)
{
    printf("%s <daliserver>\n", exe);
}

#define DALI_TIMEOUT (5*60*1000)

const char MyTestXML[] = "<MyXML><Hello>dali</Hello><Bye num=\"1\">Dali</Bye></MyXML>";

void doStuff()
{
    Owned<IRemoteConnection> conn = querySDS().connect("/Orbit", myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, DALI_TIMEOUT);
    IPropertyTree *root = conn->queryRoot();
    StringBuffer s;
    if (root->getProp("TestBranch1",s)) {
        printf("TestBranch1: read %s\n",s.str());
    }
    else {

        // save as string
        printf("TestBranch1: set (as string)\n",s.str());
        root->setProp("TestBranch1",MyTestXML);
    }
    MemoryBuffer m;
    if (root->getPropBin("TestBranch2",m)) {
        m.append((byte)0); // add a NULL to returned data
        const char *str = m.toByteArray();  
        printf("TestBranch2: read %s\n",str);
    }
    else {
        // save as raw binary
        printf("TestBranch2: set (as blob)\n",s.str());
        root->setPropBin("TestBranch2",strlen(MyTestXML),MyTestXML); // include NULL
    }
    IPropertyTree *br3 = root->queryPropTree("TestBranch3");
    if (br3) {
        printf("read TestBranch3 as tree\n");
        printf("Hello = %s\n",br3->queryProp("Hello"));
        int n = br3->getPropInt("Bye/@num");        // update
        printf("Bye num = %d\n",n);
        br3->setPropInt("Bye/@num",n+1);
    }
    else {
        // save as tree
        printf("TestBranch3: set (as tree)\n",s.str());
        br3 =  createPTreeFromXMLString(MyTestXML); // parses and creates object tree
        root->setPropTree("TestBranch3", br3);
    }
}



int main(int argc, char* argv[])
{
    
    enableMemLeakChecking(true);

    InitModuleObjects();
    EnableSEHtoExceptionMapping();

    if (argc<2) {
        usage(argv[0]);
        return -1;
    }
    SocketEndpoint dalieps(argv[1],DALI_SERVER_PORT);       // endpoint of dali server
    Owned<IGroup> group = createIGroup(1,&dalieps); 

    try {
        initClientProcess(group, DCR_Other);            // I will add a DCR_Orbit at some point
        try {
            doStuff();
        }
        catch (IException *e) {
            pexception(argv[0],e);
            e->Release();
        }
        closedownClientProcess();
    }
    catch (IException *e) {
        pexception(argv[0],e);
        e->Release();
    }
    releaseAtoms();
    return 0;
}

