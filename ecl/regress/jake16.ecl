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

rec := RECORD
unsigned1 i := 0;
string1   l := '';
  END;

rec2 := RECORD
unsigned1 i := 0;
string1   l := '';
string1   e := '';
  END;

rec2Fp := RECORD
rec2;
unsigned8 __filepos {virtual(fileposition)};
  END;

recFp := RECORD
unsigned1 i := 0;
string1   l := '';
unsigned8 __filepos {virtual(fileposition)};
  END;

ods1 := DATASET('~ds1.d00', rec, FLAT);
ods2 := DATASET('~ds2.d00', rec2, FLAT);
ods2fp := DATASET('~ds2.d00', rec2Fp, FLAT);

indexRec := RECORD
ods2fp.i;
ods2fp.__filepos;
  END;

indexRecI := RECORD
ods2fp.l;
  END;

i := INDEX(ods2fp, indexRecI, indexRec, '~test.idx');


rec trans(rec l, indexRecI r)
:=
    transform
        self := l;
    end;

j1 := JOIN(ods1, i, LEFT.l = RIGHT.l, trans(LEFT, RIGHT));

output(j1);
