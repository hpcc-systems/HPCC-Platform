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

rec :=record
string6 ucc_key;
string s;
ebcdic string4 es;
integer i;
unicode u;
end;

d1 := dataset('~in::D&B',rec,csv(terminator('\n')));
d2 := dataset('~in::D&B',rec,csv(ebcdic, terminator('\n')));

d3 := dataset('~thor::in::D&B',rec,csv(ascii, terminator('\n')));
d4 := dataset('~thor::in::D&B',rec,csv(unicode, terminator('\n')));


output(d1);
output(d2);
output(d3);
output(d4);


fpos := dataset([100,200,300,99], { unsigned fpos});

output(fetch(d1, fpos, right.fpos));
output(fetch(d2, fpos, right.fpos));
output(fetch(d3, fpos, right.fpos));
output(fetch(d4, fpos, right.fpos));

ds := dataset('ds', rec, thor);

output(ds,,'csvDefault',csv);
output(ds,,'csvAsEbcdic',csv(ebcdic));
output(ds,,'csvAsAscii', csv(ascii));
output(ds,,'csvAsUnicode', csv(unicode));
