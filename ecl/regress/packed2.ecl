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
packed unsigned2 l2;
packed unsigned1 l1;
packed    unsigned2 b2;
packed    unsigned1 b1;
    END;

personDataset := DATASET('person',personRecord,FLAT);

personRecord t1(personRecord l) := TRANSFORM
        SELF.l2 := l.b1;
        SELF.l1 := l.b2;
        SELF.b2 := l.l1;
        SELF.b1 := l.l2;
        SELF := l;
    END;

o1 := project(personDataset, t1(LEFT));
output(o1,,'out1.d00');
