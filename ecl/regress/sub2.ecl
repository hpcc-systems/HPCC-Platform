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

namesRecord :=
            RECORD
string10        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset([
        {'Hawthorn','Gavin',31},
        {'X','Z'}], namesRecord);

outr :=     RECORD
string10        a := 'Gavin'[1..4];
string10        b := namesTable.surname[1..4];
string10        c := '!'+'Gavin'[1..4]+'!';
string10        d := '!'+namesTable.surname[1..4]+'!';
string10        e := 'Gavin'[1..6];
string10        f := '!'+'Gavin'[1..6]+'!';
string10        g := trim(namesTable.surname)[4..]+'$';
string10        h := namesTable.surname[namesTable.age DIV 10..];
            END;

output(nofold(namesTable),outr,'out.d00',overwrite);
