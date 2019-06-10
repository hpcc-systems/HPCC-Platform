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

#ifndef ANAWU_HPP
#define ANAWU_HPP

#include "anacommon.hpp"
#include "workunit.hpp"
#include "eclhelper.hpp"

interface IWuScope
{
    virtual stat_type getStatRaw(StatisticKind kind, StatisticKind variant = StKindNone) const = 0;
    virtual unsigned getAttr(WuAttr kind) const = 0;
    virtual void getAttr(StringBuffer & result, WuAttr kind) const = 0;
};

interface IWuActivity;
interface IWuEdge : public IWuScope
{
    virtual IWuActivity * querySource() = 0;
    virtual IWuActivity * queryTarget() = 0;
};

interface IWuActivity : public IWuScope
{
    virtual const char * queryName() const = 0;
    virtual IWuEdge * queryInput(unsigned idx) = 0;
    virtual IWuEdge * queryOutput(unsigned idx) = 0;
    inline IWuActivity * queryInputActivity(unsigned idx)
    {
        IWuEdge * edge = queryInput(idx);
        return edge ? edge->querySource() : nullptr;
    }
    inline ThorActivityKind queryThorActivityKind()
    {
        return (ThorActivityKind) getAttr(WaKind);
    }
};

void WUANALYSIS_API analyseWorkunit(IConstWorkUnit * wu);

#endif
