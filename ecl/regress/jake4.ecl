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

namesRecord := RECORD
string3     id;
string10    surname ;
string10    forename;
string2     nl;
  END;

natRecord := RECORD
string3     id;
string10    nationality ;
string2     nl;
  END;

nameAndNatRecord := RECORD
string3     id := 1;
string10    surname := '' ;
string10    forename := '';
string10    nationality := '' ;
unsigned1 nl1 := 13;
unsigned1 nl2 := 10;
  END;

names := DATASET('names.d00', namesRecord, FLAT);
nationalities := DATASET('nat.d00', natRecord, FLAT);

sortedNames := SORT(names, surname);
sortedNats := SORT(nationalities, nationality);

nameAndNatRecord JoinTransform (namesRecord l, natRecord r)
:=
    transform
                   self := l;
                   self := r;
    end;

JoinedSmEn := join (sortedNames, sortedNats,
        LEFT.id = RIGHT.id,
        JoinTransform (LEFT, RIGHT));

output(JoinedSmEn, , 'out.d00');
