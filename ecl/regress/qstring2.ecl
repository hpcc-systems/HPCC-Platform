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

#option ('globalFold', false);
rec1 :=     RECORD
string15    forename;
string20    surname;
string5     middle := '';
            END;

rec2 :=     RECORD
qstring20   forename1;
varstring   forename2;
string20    forename3;
qstring20   surname;
            END;


test := nofold(dataset([
                {'Gavin','Hawthorn'},
                {'Richard','Drimbad'},
                {'David','Bayliss'}
                ], rec1));


rec2 t(rec1 l) := TRANSFORM
        SELF.forename1 := TRIM(l.forename) + 'a' + TRIM(l.middle);
        SELF.forename2 := TRIM(l.forename) + 'b' + TRIM(l.middle);
        SELF.forename3 := TRIM(l.forename) + 'c' + TRIM(l.middle);
        SELF:=l;
    END;

test2 := project(test, t(LEFT));

output(test2,,'out.d00',overwrite);


output(join(test2, test2(true), left.surname=right.surname, HASH));
