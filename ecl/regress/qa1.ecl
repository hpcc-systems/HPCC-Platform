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

datasetlayout :=
RECORD
  STRING9 own_1_ssn;
  unsigned integer8 fpos{virtual(fileposition)};
END;
key := INDEX(dataset([],datasetlayout),{own_1_ssn,fpos},'~thor::key::a.b.c.key');


STRING9 inssn := '' : STORED('ssn');
i := MAP(inssn<>''=> key(own_1_ssn=inssn), key(own_1_ssn=inssn[1..4]));


l := LIMIT(i,5000,keyed);


output(l);

