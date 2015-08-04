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
