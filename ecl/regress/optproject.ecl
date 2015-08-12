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

// optproject
// Adjacent projections should be combined into a single project
// Also a good example for read/filter/project optimization

testRecord := RECORD
unsigned4 person_id;
string20  per_surname;
string20  per_forename;
unsigned8 holepos;
    END;

test := DATASET('test',testRecord,FLAT);

a := table(test,{per_surname,per_forename,holepos});

testRecord2 := RECORD
string20  new_surname;
string20  new_forename;
unsigned8 holepos2;
    END;

testRecord2 doTransform(testRecord l) :=
        TRANSFORM
SELF.new_surname := l.per_surname;
SELF.new_forename := l.per_forename;
SELF.holepos2 := l.holepos * 2;
        END;

b := project(a,doTransform(LEFT));

c:= table(b,{'$'+new_surname+'$','!'+new_forename+'!',holepos14 := holepos2*7});

output(c,{new_surname+'*',holepos14+1},'out.d00');
