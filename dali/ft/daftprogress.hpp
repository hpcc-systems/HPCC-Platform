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

#ifndef DAFTPROGRESS_HPP
#define DAFTPROGRESS_HPP

#include "daft.hpp"


class DALIFT_API DaftProgress : public IDaftProgress
{
public:
    DaftProgress();

    virtual void onProgress(unsigned __int64 sizeDone, unsigned __int64 totalSize, unsigned numNodes);
    virtual void displayProgress(unsigned percentDone, unsigned secsLeft, const char * timeLeft,
                            unsigned __int64 scaledDone, unsigned __int64 scaledTotal, const char * scale,
                            unsigned kbPerSecondAve, unsigned kbPerSecondRate,
                            unsigned numNodes) = 0;
    virtual void displaySummary(const char * timeTaken, unsigned kbPerSecond) = 0;
    virtual void setRange(unsigned __int64 sizeReadBefore, unsigned __int64 totalSize, unsigned _totalNodes);

protected:
    void formatTime(char * buffer, unsigned secs);

protected:
    enum { MaxSamples = 10 };
    cycle_t startTime;
    offset_t startSize;
    unsigned scale;
    const char * scaleUnit;
    double cycleToNanoScale;
    unsigned numSamples;
    unsigned nextSample;
    unsigned totalNodes;
    cycle_t savedTime[MaxSamples];
    offset_t savedSize[MaxSamples];
};


class DALIFT_API DemoProgress : public DaftProgress
{
public:
    virtual void displayProgress(unsigned percentDone, unsigned secsLeft, const char * timeLeft,
                            unsigned __int64 scaledDone, unsigned __int64 scaledTotal, const char * scale,
                            unsigned kbPerSecondAve, unsigned kbPerSecondRate, unsigned numNodes);
    virtual void displaySummary(const char * timeTaken, unsigned kbPerSecond);
};

#endif

