/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

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

namesRecord :=
            RECORD
string10        id1;
string10        id2;
            END;

namesTable := dataset('x',namesRecord,FLAT);

indexKey := INDEX(namesTable, { id1 }, { id2 }, 'i1');

DISTRIBUTE(namesTable, indexKey, LEFT.id2 = RIGHT.id2);
