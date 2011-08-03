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

rs :=
RECORD
    string a;
    integer iField;
END;
ds := DATASET([{'a', 500}, {'a', 10}, {'g', 5}, {'q', 16}, {'e', 2}, {'d', 12}, {'c', 1}, {'f', 17}, {'a', 500}, {'a', 10}, {'g', 5}, {'q', 16}, {'e', 2}, {'d', 12}, {'c', 1}, {'f', 17}], rs);

dist := DISTRIBUTE(NOFOLD(ds), HASH(iField));

#uniquename(newRS)
#uniquename(temp_id)
%newRS% :=
RECORD
    ds;
    INTEGER %temp_id%;
END;

#uniquename(s)
#uniquename(p)
#uniquename(sequence)
    %s% := SORT(dist, ifield, LOCAL);


%newRS% %sequence%(ds L, INTEGER C) :=
TRANSFORM
    SELF.%temp_id% := C;
    SELF := L;
END;
%p% := PROJECT(%s%, %sequence%(LEFT, COUNTER));

#uniquename(cnt)
%cnt% := COUNT(ds);
#uniquename(keepem)
#uniquename(Smallest)
#uniquename(FirstQ)
#uniquename(MeanQ)
#uniquename(ThirdQ)
#uniquename(Biggest)
%Smallest% := 1;
%FirstQ% := %cnt% div 4;
%MeanQ% := %cnt% div 2;
%ThirdQ% := %cnt% * 3 div 4;
%Biggest% := %cnt%;
SET OF INTEGER %keepem% := [%Smallest%, %FirstQ%, %MeanQ%, %ThirdQ%, %Biggest%];
#uniquename(stats)
%stats% := %p%(%temp_id% IN %keepem%);

#uniquename(slimmer)
ds %slimmer%(%newRS% L) :=
TRANSFORM
    SELF := L;
END;
#uniquename(noTempID)
%noTempID% := PROJECT(%stats%, %slimmer%(LEFT));
output(choosen(SORT(%noTempID%, ifield),100));
