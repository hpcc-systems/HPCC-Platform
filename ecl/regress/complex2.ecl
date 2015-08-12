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
SMatch(string s1, string s2) :=  MAP(s1=s2 =>0, 1);

NSMatch(string s1, string s2) :=
  IF ( s1='' or s2='',0, SMatch(s1,s2));

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
integer2        age2 := 25;
            END;

namesTable := dataset('x',namesRecord,FLAT);


namesRecord t(namesRecord l) := TRANSFORM
    SELF.age := IF(l.age > 20, NSMatch(l.forename,l.forename)+NSMatch(l.forename,l.forename), 1);
    SELF.age2 := IF(l.age > 10, NSMatch(l.surname,l.forename)+NSMatch(l.surname,l.forename), NSMatch(l.surname,l.forename)) + NSMatch(l.forename,l.forename);
    SELF := l;
    END;

x := project(namesTable, t(LEFT));
output(x);

