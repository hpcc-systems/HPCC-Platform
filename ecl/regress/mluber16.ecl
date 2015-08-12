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
