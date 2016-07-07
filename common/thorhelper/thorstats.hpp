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

#include "eclhelper.hpp"
#include "jstats.h"

class ThorSectionTimer : public CSimpleInterfaceOf<ISectionTimer>
{
public:
    static ThorSectionTimer * createTimer(CRuntimeStatisticCollection & _stats, const char * _name);

    bool matches(const char * _name) const
    {
        return strsame(name, _name);
    }

    virtual unsigned __int64 getStartCycles();
    virtual void noteSectionTime(unsigned __int64 startCycles);

protected:
    ThorSectionTimer(const char * _name, CRuntimeStatistic & _occurences, CRuntimeStatistic & _elapsed);

private:
    CRuntimeStatistic & occurences;
    CRuntimeStatistic & elapsed;
    StringAttr name;
};

#endif
