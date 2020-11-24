/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC SystemsÂ®.

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


import dbglog from Std.System.log;

namesRecord :=
            RECORD
unsigned        id;
string          name;
            END;

names2 := DEDUP(NOCOMBINE(DATASET([{1,'Gavin'},{2,'Bill'},{3,'John'},{4,'Gerald'},{5,'Fluffy'}], namesRecord)),id);
names1 := DEDUP(NOCOMBINE(DATASET([{1,'Oscar'},{2,'Charles'},{3,'Freddie'},{4,'Winifred'},{5,'Bouncer'}], namesRecord)), id);

s := nofold(sort(names2, name));
s2 := LIMIT(s, 1);

j := join(names1, s, left.id = right.id, transform(namesRecord, self.id := left.id + s2[2].id; self.name := right.name), left outer, keep(1));

output(names2);
dbglog('Hello ' + (string)names2[3].name + ' and ' + (string)count(j) + 'again');
