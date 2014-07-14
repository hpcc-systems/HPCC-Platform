/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

//nothor

#option ('checkAsserts',false);
import $.Common.TextSearch;
import $.Common.TextSearchQueries;

q1 := TextSearchQueries.WordTests;

output(TextSearch.executeBatchAgainstWordIndex(q1, false, 'thorlcr', 0x80000000)); // 0x80000000 means always optimize
