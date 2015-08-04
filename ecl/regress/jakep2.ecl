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

#option ('optimizeGraph', false);

ppersonRecord := RECORD
string3     id := '000';
string10    surname := '';
string10    forename := '';
unsigned1 nl1 := 13;
unsigned1 nl2 := 10;
  END;

pperson := DATASET('names.d00', ppersonRecord, FLAT, OPT);
// pperson := DATASET('%jaketest%', ppersonRecord, FLAT, OPT);
pperson2 := DATASET('out1.d00', ppersonRecord, FLAT);
pperson3 := DATASET('names3.d00', ppersonRecord, FLAT);
pperson4 := DATASET('names4.d00', ppersonRecord, FLAT);

smiths := pperson(Stringlib.StringToLowerCase(surname)='Smith.....');


// s1 := sort(pperson+pperson2, surname);
s1 := sort(pperson, surname);
s2 := sort(s1, forename);

// output(s1, ,'out1.d00');
//output(pperson2, ,'out2.d00');

count(s1):PERSIST('xxx');
