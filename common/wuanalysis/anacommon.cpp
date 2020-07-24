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
#include <string.h>
#include "anacommon.hpp"
#include "workunit.hpp"

int compareIssuesCostOrder(CInterface * const * _l, CInterface * const * _r)
{
    const PerformanceIssue * l = static_cast<const PerformanceIssue *>(*_l);
    const PerformanceIssue * r = static_cast<const PerformanceIssue *>(*_r);
    return l->compareCost(*r);
}

int PerformanceIssue::compareCost(const PerformanceIssue & other) const
{
    if (cost == other.cost)
        return 0;
    else
        return cost > other.cost ? -1 : +1;
}

void PerformanceIssue::print() const
{
    StringBuffer out;
    formatStatistic(out, cost, SMeasureTimeNs);
    printf("[%s] E%d \"%s\" %s ", out.str(), errorCode,  comment.str(), scope.str());
    if (filename.length()>0)
        printf("%s", filename.str());
    if (line>0 && column>0)
        printf("(%u,%u)", line, column);
    printf("\n");
}

void PerformanceIssue::createException(IWorkUnit * wu)
{
    ErrorSeverity mappedSeverity = wu->getWarningSeverity(errorCode, (ErrorSeverity)SeverityWarning);
    if (mappedSeverity == SeverityIgnore)
        return;

    Owned<IWUException> we = wu->createException();
    we->setExceptionCode(errorCode);
    we->setSeverity(mappedSeverity);
    we->setScope(scope.str());
    we->setPriority((unsigned) statUnits2msecs(cost));
    if (line>0 && column>0)
    {
        we->setExceptionLineNo(line);
        we->setExceptionColumn(column);
    }
    if (filename.length()>0)
        we->setExceptionFileName(filename);
    StringBuffer s(comment);        // Append scope to comment as scope column is not visible in ECLWatch
    s.appendf(" (%s)", scope.str());
    we->setExceptionMessage(s.str());
    we->setExceptionSource("Workunit Analyzer");
}

void PerformanceIssue::set(AnalyzerErrorCode _errorCode, stat_type _cost, const char * msg, ...)
{
    cost = _cost;
    errorCode = _errorCode;
    va_list args;
    va_start(args, msg);
    comment.valist_appendf(msg, args);
    va_end(args);
}

void PerformanceIssue::setLocation(const char * definition)
{
    const char *p1 = strchr(definition,'(');
    if (!p1)
        return;
    const char *comma = strchr(p1+1,',');
    if (!comma)
        return;
    const char *p2 = strchr(comma+1,')');
    if (!p2)
        return;
    if (p1>definition) // have filename
        filename.append(p1-definition, definition);
    line = atoi(p1+1);
    column = atoi(comma+1);
    if (line==0 || column==0)
    {
        line=0;
        column=0;
        IERRLOG("Error parsing Definition for line and column: %s", definition);
    }
};
