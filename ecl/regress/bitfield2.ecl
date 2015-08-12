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


namesRecord :=
            RECORD
string20        surname;
bitfield10_8    age;
bitfield52      age2;
bitfield10      age3;
bitfield52      age4;
bitfield1       isOkay;
bitfield2       dead;       // y/N/maybe
varstring1          okay;
bitfield28      largenum;
bitfield1       extra;
bitfield63      gigantic;
bitfield1       isflag;
            END;

namesTable := dataset('x',namesRecord,FLAT);

namesRecord t(namesRecord l) :=
        TRANSFORM
            SELF.age := IF(l.age > (integer8)12, 10,12);
            SELF.dead := (integer)l.surname;
            SELF.surname := (string)l.largenum;
            SELF := l;
        END;

z := project(namesTable, t(LEFT));

output(z);//,,'out.d00');
