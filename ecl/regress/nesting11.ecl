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

thina :=     RECORD
unsigned4       f1;
string20        f2;
            END;

thin :=     RECORD
                thina;
                ifblock(self.f1 > 10)
qstring10           f3;
                end;
            END;

fat :=      RECORD
                thin;
string30        x2;
thina           g1;
unsigned        temp;
unsigned        g3;
                ifblock(self.g3 > 100)
unsigned            g4;
                end;
            END;

inFat := dataset('fat', fat, thor);
x := table(inFat, { g1 });
output(x);
output(table(inFat, { slim := g1 }));

