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

ppersonRecord := RECORD
string10    surname ;
varstring10 forename;
unicode     uf1;
varunicode10 uf2;
decimal10_2  df;
integer4    nl;
  END;


string x(string a) := (trim(a) + ',');


pperson := DATASET('in.d00', ppersonRecord, FLAT);

output(pperson, {hash(surname),hash(forename),hash(nl),hash(x(surname)),hash(surname,forename)}, 'outh.d00');
output(pperson, {hashcrc(surname),hashcrc(forename),hashcrc(nl),hashcrc(x(surname)),hashcrc(surname,forename)}, 'outc.d00');
output(pperson, {hash(uf1),hash(uf2),hash(df)});
output(pperson, {hash32(surname,forename)});


output(pperson, {hash64(surname),hash64(forename),hash64(uf1),hash64(uf2),hash64(df),hash64(nl)});
output(pperson, {hash32(surname),hash32(forename),hash32(uf1),hash32(uf2),hash32(df),hash32(nl)});

hash('hi')-hash('hi ');
hash(10)-hash((integer5)10);
