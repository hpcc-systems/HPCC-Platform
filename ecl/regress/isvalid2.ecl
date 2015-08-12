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

#option ('foldAssign', false);
#option ('globalFold', false);
//good
SpecialReal := TYPE
  export real       load(string6 physical) := (real)physical;
  export string6    store(real logical) := (string6)logical;
END;

NullSigned2 := TYPE
  export boolean    getisvalid(integer2 physical) := ((unsigned2)physical != 32768);
  export integer2   load(integer2 physical) := physical*10;
  export integer2   store(integer2 logical) := logical/10;
END;

personRecord := RECORD
decimal8_2  dec1 := 0;
udecimal8_2 dec2 := 1;
decimal9_3  dec3 := 2;
real4       r1 := 1;
real8       r2 := 2;
SpecialReal s1 := 0.0;
NullSigned2 s2 := -32768;
END;

pperson := DATASET([{1},
{
0,
0,
0,
transfer(x'0100807F', real4),
transfer(x'010000000000F07F', real8)
}
], personRecord);

output(pperson, { ISVALID(dec1), ISVALID(dec2), ISVALID(dec3), ISVALID(r1), ISVALID(r2), ISVALID(s1), ISVALID(s2),

transfer(transfer(dec1,string5)[3],integer1) IN [0,1,2,3,4],
ISNULL(dec3), ISNULL(r1)
 }, 'out.d00');

