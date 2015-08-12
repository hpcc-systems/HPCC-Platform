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

#option ('targetClusterType', 'roxie');

import * from sq;
DeclareCommon();

//UseStandardFiles

o1 := output(table(sqSimplePersonBookIndex, { dataset books := sqSimplePersonBookIndex.books, sqSimplePersonBookIndex.surname, count(group) }, sqSimplePersonBookIndex.surname[2..], few));

o4 := output(table(sqSimplePersonBookIndex, { dataset books := sqSimplePersonBookIndex.books, sqSimplePersonBookIndex.surname, count(group) }, sqSimplePersonBookIndex.surname, few));

o2 := output(table(sqPersonBookExDs, { dataset books := sqPersonBookExDs.books, sqPersonBookExDs.surname, sqPersonBookExDs.filepos, count(group) }, sqPersonBookExDs.surname, few));

o3 := output(table(sqSimplePersonBookIndex.books, { id }));

sequential(o1, o2, o3, o4);