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




