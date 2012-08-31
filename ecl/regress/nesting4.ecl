/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
unsigned4       f1{virtual(fileposition)};
string20        f2;
                ifblock(self.f1 > 10)
qstring10           f3;
                end;
            END;

fat :=      RECORD
                thin;
unsigned        temp;
            END;

inFat := dataset('fat', fat, thor);

fat2 :=     RECORD
unsigned        temp2 := 0;
                inFat;
            END;


o := table(inFat, fat2);

output(o);
