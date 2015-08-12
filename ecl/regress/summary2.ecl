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


/* Demo hql file using the CSV stuff to summarise my account transactions.... */

accountRecord :=
            RECORD
string10        strDate;
string40        category;
real8           credit;
decimal8_2      balance;
            END;

accountTable := dataset('~account',accountRecord,CSV);

summaryTable := table(accountTable, { decimal8_2 value := SUM(group,credit), unsigned2 cnt := COUNT(GROUP), category}, category);

sortedSummary := sort(summaryTable, -value);

output(sortedSummary,,'gavin::x::summary',CSV(TERMINATOR('\r\n')));
