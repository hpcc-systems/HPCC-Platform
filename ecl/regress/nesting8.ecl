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

import dt;

thin :=     RECORD
unsigned4       f1;
string20        f2;
            END;

inThin := dataset('thin', thin, thor);

fat1 :=     RECORD
thin            g1;
thin            g2;
unsigned        g3 := inThin.f1;
                ifblock(self.g3 > 100)
unsigned            g4;
                end;
            END;


fat2 :=     RECORD
thin            g1;
thin            g2;
unsigned        g3 := inThin.f1;
            END;

inFat1 := dataset('fat1', fat1, thor);

x := sort(inFat1, g3*2, g4);
output(x);

inFat2 := dataset('fat2', fat2, thor);
y := sort(inFat2, g1.f1, g2.f2);
output(y);

z := sort(inFat2, g3);
output(z);
