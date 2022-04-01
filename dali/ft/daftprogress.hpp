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
    virtual void setFileAccessCost(double fileAccessCost) = 0;
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
    virtual void setFileAccessCost(double fileAccessCost) {};
};

#endif

