/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

#pragma warning (disable : 4786)
#include "InfoCacheReader.hpp"

bool CInfoCache::isCachedInfoValid(unsigned timeOutSeconds)
{
    CDateTime timeNow;
    timeNow.setNow();
    return timeNow.getSimple() <= timeCached.getSimple() + timeOutSeconds;;
}

void CInfoCacheReaderThread::threadmain()
{
    PROGLOG("CInfoCacheReaderThread %s started, rebuilding every %ds.", name.get(), autoRebuildSeconds);
    unsigned int autoRebuildMillSeconds = 1000*autoRebuildSeconds;
    while (!stopping)
    {
        if (active)
        {
            try
            {
                CCycleTimer timer;
                Owned<CInfoCache> info = infoCacheReader->read();

                CriticalBlock b(crit);
                infoCache.setown(info.getClear());

                // if 1st and getActivityInfo blocked, release it.
                if (first)
                {
                    first = false;
                    if (firstBlocked)
                    {
                        firstBlocked = false;
                        firstSem.signal();
                    }
                }
            }
            catch(IException *e)
            {
                StringBuffer msg;
                IERRLOG("Exception %d:%s in CInfoCacheReaderThread(%s)::run", e->errorCode(), e->errorMessage(msg).str(), name.get());
                e->Release();
            }
            catch(...)
            {
                IERRLOG("Unknown exception in CInfoCacheReaderThread(%s)::run", name.get());
            }
        }

        waiting = true;
        if (!sem.wait(autoRebuildMillSeconds))
            waiting = false;
    }
}
