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

