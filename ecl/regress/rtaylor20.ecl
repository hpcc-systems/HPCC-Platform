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

import std.system.thorlib;

RandomSample(InFile,UID_Field,SampleSize) := MACRO

#uniquename(Layout_Plus_RecID)
%Layout_Plus_RecID% := RECORD
  UNSIGNED8 RecID := 0;
InFile.UID_Field;
END;

#uniquename(InTbl)
%InTbl% := TABLE(InFile,%Layout_Plus_RecID%);

#uniquename(IDRecs)
%Layout_Plus_RecID% %IDRecs%(%Layout_Plus_RecID% L, 
                           %Layout_Plus_RecID% R) := TRANSFORM
  SELF.RecID := IF(L.RecID=0,thorlib.node()+1,L.RecID+thorlib.nodes());
SELF := R;
END;

#uniquename(UID_Recs)
%UID_Recs% := ITERATE(%InTbl%,%IDRecs%(LEFT,RIGHT),LOCAL);


//generate 
#uniquename(WholeSet)
%WholeSet% := COUNT(InFile) : GLOBAL;
output(%WholeSet%);

#uniquename(BlankSet)
%BlankSet%  := DATASET([{0,0.0}],{unsigned8 seq, real8 score});

#uniquename(SelectEm)
typeof(%BlankSet%) %SelectEm%(%BlankSet% L, integer c) := transform
s := ((random() % 100000) + 1) / 100000; //get 5-place decimal
SELF.score := s;
SELF.seq   := 0;
  end;

#uniquename(selected)
%selected% := normalize( %BlankSet%, SampleSize, %SelectEm%(left, counter));

#uniquename(SelectEm2)
typeof(%BlankSet%) %SelectEm2%(%BlankSet% L) := transform
x := round(%WholeSet% * l.score); //multiply by # recs in & round
SELF.seq   := if(x=0,1,x);
SELF := l;
  end;

#uniquename(selected2)

%selected2% := project( %selected%, %SelectEm2%(left));

output(%selected2%);

#uniquename(SetSelectedRecs)
%SetSelectedRecs% := SET(%selected2%,seq);
output(%UID_Recs%(RecID IN %SetSelectedRecs% ));

ENDMACRO;

////////////////////////////////////////////////////////////////
SomeFile := DATASET([{'A'},{'B'},{'C'},{'D'},{'E'},
                     {'F'},{'G'},{'H'},{'I'},{'J'},
                     {'K'},{'L'},{'M'},{'N'},{'O'},
                     {'P'},{'Q'},{'R'},{'S'},{'T'},
                     {'U'},{'V'},{'W'},{'X'},{'Y'}],
{STRING1 Letter});
ds := distribute(SomeFile,random());
RandomSample(ds,Letter,5)
