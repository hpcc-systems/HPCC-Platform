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

#ifndef ANACOMMON_HPP
#define ANACOMMON_HPP

#include "jliball.hpp"
#include "wuattr.hpp"
#include "eclhelper.hpp"
#include "anaerrorcodes.hpp"

#ifdef WUANALYSIS_EXPORTS
    #define WUANALYSIS_API DECL_EXPORT
#else
    #define WUANALYSIS_API DECL_IMPORT
#endif

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
    virtual const char * getFullScopeName(StringBuffer & fullScopeName) const = 0;
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

class PerformanceIssue : public CInterface
{
public:
    int compareCost(const PerformanceIssue & other) const;
    void print() const;
    void createException(IWorkUnit * we);

    void set(AnalyzerErrorCode _errorCode, stat_type _cost, const char * msg, ...) __attribute__((format(printf, 4, 5)));
    void setLocation(const char * definition);
    void setScope(const char *_scope) { scope.set(_scope); }
    stat_type getCost() const         { return cost; }

private:
    AnalyzerErrorCode errorCode = ANA_GENERICERROR_ID;
    StringBuffer filename;
    unsigned line = 0;
    unsigned column = 0;
    StringAttr scope;
    stat_type cost = 0;      // number of nanoseconds lost as a result.
    StringBuffer comment;
};

extern int compareIssuesCostOrder(CInterface * const * _l, CInterface * const * _r);

#endif
