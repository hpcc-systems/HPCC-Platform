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

r1 :=        RECORD
unsigned4        r1f1;
            END;

r2 :=        RECORD
unsigned4        r2f1;
r1               r2f2;
            END;



tableFormat := record
integer2    i2;
integer8    i8;
unsigned2   u2;
unsigned8   u8;
            ifblock(self.i2 & 10 != 0)
string10        s20;
varstring10     v20;
qstring10       q20;
data30          d30;
            end;
string      sx;
real4       r4;
real8       r8;
decimal10_2     d10_2;
udecimal10_2    ud10_2;
bitfield3   b3;
r2          nested;
        end;


d := DATASET('table', tableFormat, FLAT);

output(d);
