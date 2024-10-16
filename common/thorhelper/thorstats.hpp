/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

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



#ifndef THORSTATS_HPP
#define THORSTATS_HPP

#include "thorhelper.hpp"
#include "eclhelper.hpp"
#include "jstats.h"

// If you add values to this enum, update ThorSectionTimer::createTimer so that it can handle the new value
enum ThorStatOption
{
    ThorStatDefault,
    ThorStatGenericCache,
    ThorStatMax // Marker, always last
};

class THORHELPER_API ThorSectionTimer : public CSimpleInterfaceOf<ISectionTimer>
{
public:
    // Creates an instance of the timer that tracks the statistics cited in nestedSectionStatistics
    static ThorSectionTimer * createTimer(CRuntimeStatisticCollection & _stats, const char * _name, const StatisticsMapping & nestedSectionStatistics);
    // Creates an instance of the timer that tracks the statistics referenced through the ThorStatOption enum
    static ThorSectionTimer * createTimer(CRuntimeStatisticCollection & _stats, const char * _name, ThorStatOption statOption);
    // Default instantiation of a timer; uses defaultNestedSectionStatistics
    static ThorSectionTimer * createTimer(CRuntimeStatisticCollection & _stats, const char * _name);

    bool matches(const char * _name) const
    {
        return strsame(name, _name);
    }

    virtual unsigned __int64 getStartCycles();
    virtual void noteSectionTime(unsigned __int64 startCycles);
    virtual void addStatistic(__int64 kind, unsigned __int64 value) override;
    virtual void setStatistic(__int64 kind, unsigned __int64 value) override;
    virtual void mergeStatistic(__int64 kind, unsigned __int64 value) override;

protected:
    ThorSectionTimer(const char * _name, CRuntimeStatisticCollection & _stats);

private:
    StringAttr name;
    CRuntimeStatisticCollection & stats;
};

#endif
