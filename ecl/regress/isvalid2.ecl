/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

