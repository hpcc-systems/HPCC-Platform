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

#include "jliball.hpp"

#include "platform.h"

#include "jlib.hpp"
#include "jio.hpp"

#include "jmutex.hpp"
#include "jfile.hpp"
#include "jsocket.hpp"
#include "jdebug.hpp"

#include "fterror.hpp"
#include "dadfs.hpp"
#include "rmtspawn.hpp"
#include "filecopy.ipp"
#include "jptree.hpp"
#include "daft.hpp"
#include "daftcfg.hpp"
#include "fterror.hpp"
#include "daftsize.hpp"
#include "daftmc.hpp"

bool processSizesCommand(ISocket *, MemoryBuffer & cmd, MemoryBuffer & result)
{
    LOG(MCdebugProgress, unknownJob, "Start gather remote file sizes");
    unsigned numFiles;
    StringBuffer path;

    cmd.read(numFiles);
    for (unsigned idx=0; idx < numFiles; idx++)
    {
        RemoteFilename name;
        name.deserialize(cmd);

        //NB: File must be directly accessible - otherwise this should not be called.
        name.getPath(path.clear());
        OwnedIFile file = createIFile(path.str());
        assertex(file);
        __int64 size = file->size();
        result.append(size);
    }

    LOG(MCdebugProgress, unknownJob, "Stop gather remote file sizes");
    return true;
}




