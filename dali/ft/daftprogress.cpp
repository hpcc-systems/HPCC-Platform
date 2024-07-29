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
#include <limits.h>

#include "jlib.hpp"
#include "jio.hpp"

#include "jmutex.hpp"
#include "jfile.hpp"
#include "jsocket.hpp"

#include "daclient.hpp"

#include "fterror.hpp"
#include "dadfs.hpp"
#include "rmtspawn.hpp"
#include "filecopy.hpp"
#include "fttransform.hpp"
#include "daft.hpp"
#include "daftmc.hpp"
#include "dalienv.hpp"

#include "jprop.hpp"
#include "jptree.hpp"
#include "jlog.hpp"
#include "daftprogress.hpp"

DaftProgress::DaftProgress() 
{ 
    startTime = get_cycles_now(); 
    scale = 1; 
    scaleUnit = "bytes"; 
    cycleToNanoScale = cycle_to_nanosec(1000000000) / 1000000000.0;
    numSamples = 0;
    nextSample = 0;
    totalNodes = 0;
}

void DaftProgress::formatTime(char * buffer, unsigned secs)
{
    if (secs >= 5400)
        sprintf(buffer, "%d:%02d:%02d", secs/3600, (secs%3600)/60, secs%60);
    else if (secs >= 120)
        sprintf(buffer, "%dm %ds", secs/60, secs%60);
    else
        sprintf(buffer, "%d secs", secs);
}

void DaftProgress::onProgress(unsigned __int64 sizeDone, unsigned __int64 totalSize, unsigned numNodes, unsigned __int64 numReads, unsigned __int64 numWrites)
{
    cycle_t nowTime = get_cycles_now();
    savedTime[nextSample] = nowTime;
    savedSize[nextSample] = sizeDone;
    nextSample++;
    if (nextSample == MaxSamples)
        nextSample = 0;
    if (numSamples != MaxSamples)
        ++numSamples;

    if (sizeDone != startSize)
    {
        cycle_t elapsedTime = nowTime - startTime;
        cycle_t timeLeft = (cycle_t)(((double)elapsedTime) * (double)(totalSize - sizeDone) / (double)(sizeDone-startSize));
        unsigned __int64 msGone = cycle_to_nanosec(elapsedTime)/1000000;
        
        unsigned firstSample = (nextSample+MaxSamples-numSamples) % MaxSamples;
        offset_t recentSizeDelta = sizeDone - savedSize[firstSample];
        unsigned __int64 recentTimeDelta = cycle_to_nanosec(nowTime - savedTime[firstSample])/1000000;

        unsigned secsLeft = (unsigned)(timeLeft * cycleToNanoScale /1000000000);
        char temp[20];
        formatTime(temp, secsLeft);

        unsigned percentDone = (unsigned)(totalSize ? (sizeDone*100/totalSize) : 100);

        unsigned __int64 kbPerSecond = (sizeDone-startSize) / 1024;
        if (msGone) // if took no time, leave as max total kb.
            kbPerSecond = (kbPerSecond * 1000) / msGone;

        displayProgress(percentDone, secsLeft, temp,
                        sizeDone/scale, totalSize/scale, scaleUnit,
                        (unsigned)kbPerSecond,
                        (unsigned)(recentTimeDelta ? recentSizeDelta / recentTimeDelta : 0), numNodes, numReads, numWrites);

        if (sizeDone == totalSize)
        {
            formatTime(temp, (unsigned)(msGone/1000));
            displaySummary(temp, (unsigned)kbPerSecond);
        }
    }
}


void DaftProgress::setRange(unsigned __int64 sizeReadBefore, unsigned __int64 totalSize, unsigned _totalNodes)
{
    cycle_t nowTime = get_cycles_now();
    numSamples = 1;
    nextSample = 1;
    savedTime[0] = nowTime;
    savedSize[0] = sizeReadBefore;
    startTime = nowTime;    // reset start time when the job actually starts copying.
    startSize = sizeReadBefore;
    totalNodes = _totalNodes;
    if (totalSize >= 50000000)
    {
        scale = 1000000;
        scaleUnit = "MB";
    }
    else if (totalSize > 500000)
    {
        scale = 1000;
        scaleUnit = "KB";
    }
    else
    {
        scale = 1;
        scaleUnit = "bytes";
    }
}

//---------------------------------------------------------------------------

void DemoProgress::displayProgress(unsigned percentDone, unsigned secsLeft, const char * timeLeft,
                            unsigned __int64 scaledDone, unsigned __int64 scaledTotal, const char * scale,
                            unsigned kbPerSecondAve, unsigned kbPerSecondRate, unsigned numNodes)
{

    LOG(MCdebugProgress, "Progress: %d%% done, %s left.  (%" I64F "d/%" I64F "d%s @Ave(%dKB/s) Rate(%dKB/s) [%d/%d]",
            percentDone, timeLeft, scaledDone, scaledTotal, scale, kbPerSecondAve, kbPerSecondRate, numNodes, totalNodes);
}

void DemoProgress::displaySummary(const char * timeTaken, unsigned kbPerSecond)
{
    LOG(MCdebugProgress, "Summary: Total time taken %s, Average transfer %dKB/sec", timeTaken, kbPerSecond);
}
