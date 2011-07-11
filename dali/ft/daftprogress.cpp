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

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/dali/ft/daftprogress.cpp $ $Id: daftprogress.cpp 62376 2011-02-04 21:59:58Z sort $");

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

void DaftProgress::onProgress(unsigned __int64 sizeDone, unsigned __int64 totalSize, unsigned numNodes)
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
        displayProgress((unsigned)(sizeDone*100/totalSize), secsLeft, temp, 
                        sizeDone/scale,totalSize/scale,scaleUnit, 
                        (unsigned)(msGone ? (sizeDone-startSize)/msGone : 0),
                        (unsigned)(recentTimeDelta ? recentSizeDelta / recentTimeDelta : 0), numNodes);

        if (sizeDone == totalSize)
        {
            formatTime(temp, (unsigned)(msGone/1000));
            displaySummary(temp, (unsigned)((totalSize - startSize)/msGone));
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

    LOG(MCdebugProgress, unknownJob, "Progress: %d%% done, %s left.  (%"I64F"d/%"I64F"d%s @Ave(%dKB/s) Rate(%dKB/s) [%d/%d]", 
            percentDone, timeLeft, scaledDone, scaledTotal, scale, kbPerSecondAve, kbPerSecondRate, numNodes, totalNodes);
}

void DemoProgress::displaySummary(const char * timeTaken, unsigned kbPerSecond)
{
    LOG(MCdebugProgress, unknownJob, "Summary: Total time taken %s, Average transfer %dKB/sec", timeTaken, kbPerSecond);
}
