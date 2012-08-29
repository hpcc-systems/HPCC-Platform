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

#include <jliball.hpp>

#include "daclient.hpp"
#include "dadfs.hpp"
#include "dasds.hpp"
#include "dalienv.hpp"
#include "fileview.hpp"
#include "dllserver.hpp"

Semaphore sem;


bool myAbortHandler()
{
    sem.signal();
    return false;
}


int main(int argc, const char *argv[])
{
    InitModuleObjects();
    if (argc < 4)
    {
        printf("fvserver <dali-server> <queue> <cluster>");
        return 1;
    }
    
    Owned<IGroup> serverGroup = createIGroup(argv[1],DALI_SERVER_PORT);
    initClientProcess(serverGroup, DCR_Other, 9123, NULL, NULL, MP_WAIT_FOREVER);
    setPasswordsFromSDS(); 

    LocalAbortHandler localHandler(myAbortHandler);
    startRemoteDataSourceServer(argv[2], argv[3]);
    sem.wait();
    stopRemoteDataSourceServer();

    closeDllServer();
    closeEnvironment();
    closedownClientProcess();
    releaseAtoms();
    return 0;
}

