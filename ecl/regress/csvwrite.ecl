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



allRecord := record
boolean b;
little_endian integer2  li2;
little_endian integer3  li3;
little_endian integer li;
big_endian integer2 bi2;
big_endian integer3 bi3;
big_endian integer  bi;
little_endian unsigned2 lu2;
little_endian unsigned3 lu3;
little_endian unsigned lu;
big_endian unsigned2    bu2;
big_endian unsigned3    bu3;
big_endian unsigned bu;
real4   r4;
real8 r8;
string10        s10;
ebcdic string10 es10;
varstring8      vs8;
data16          d16;
qstring         qs;
utf8            uf8;
unicode20       u20;
varunicode19    vu19;
decimal8_2      d82;
udecimal10_3    ud10_3;
end;

d := DATASET('d', allRecord , FLAT);

output(d,,'o1',csv);
output(d,,'o2',xml);
