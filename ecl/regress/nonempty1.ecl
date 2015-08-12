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

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
unsigned8       filepos{virtual(fileposition)};
            END;

d := dataset('x',namesRecord,FLAT);

i1 := index(d, { d } ,'\\home\\person.name_first.key1');

output(nonempty(i1(surname='Hawthorn'), i1(surname='Smith'), i1(surname='Drimbad'), i1(surname='Jones'))) : onwarning(4523, ignore);

output(nonempty(i1(false), i1(surname='Smithe'), i1(false), i1(surname='Johnson')));

output(nonempty(i1(false)));

output(nonempty(i1(surname = 'James')));
