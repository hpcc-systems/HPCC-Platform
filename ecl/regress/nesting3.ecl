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


thin :=     RECORD
unsigned4       f1;
string20        f2;
                ifblock(self.f1 > 10)
qstring10           f3;
                end;
            END;

fat :=      RECORD
thin            g1;
thin            g2;
thin            g3;
            END;


inThin := dataset('thin', thin, thor);
inFat := dataset('fat', fat, thor);

fat fillit(fat l, thin r, unsigned c) := transform
        self.g1 := if(c = 1, r, l.g1);
        self.g2 := if(c = 2, r, l.g2);
        self.g3 := if(c = 3, r, l.g3);
    end;

o := denormalize(inFat, inThin, left.g1.f1 = right.f1, fillit(left, right, counter));

output(o);
