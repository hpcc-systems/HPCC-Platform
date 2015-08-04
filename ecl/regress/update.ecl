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

personRecord := RECORD
unsigned4 personid;
string1 sex;
string33 forename;
string33 surname;
unsigned8 filepos{virtual(fileposition)};
    END;

personDataset := DATASET('person',personRecord,thor);
i := index(personDataset, { personDataset }, 'i');

output(personDataset,,'abc',update);
build(i,update);


s := sort(personDataset, surname);
x1 := s : persist('persist::x1');
d := dedup(s, surname);

s2 := sort(d, forename, surname);
output(s2+x1,,'abcUpdate',update);
output(s2+x1,,'abcPlain');
output(s2+x1,,'abcOverwrite',overwrite);
output(s2+x1,,'abcNoOverwrite',nooverwrite);