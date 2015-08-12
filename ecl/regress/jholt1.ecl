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

#option ('childQueries', true);

Layout_T1 := RECORD
   INTEGER x1;
INTEGER x2;
STRING s3;
END;

Data_T1 := DATASET([{1,2,'a'},{1,3,'a2'},{3,5,'b'}],Layout_T1);

LAYOUT_T0 := RECORD
     INTEGER y1;
DATASET(Layout_T1) z;
END;

Data_S0 := DATASET([{3},{2},{6},{5}],{INTEGER y});

LAYOUT_T0 makeT0({INTEGER y} l) := TRANSFORM
   self.y1 := l.y;
   self.z := Data_T1;
END;

Layout_T0 filterZ(Layout_T0 l) := TRANSFORM
   self.z := l.z(l.y1 between l.z.x1 and l.z.x2);
self := l;
END;

D_1 := project(Data_S0,makeT0(LEFT));
// output(D_1);
output(project(D_1,filterZ(LEFT)));
