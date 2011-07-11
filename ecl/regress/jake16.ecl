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
