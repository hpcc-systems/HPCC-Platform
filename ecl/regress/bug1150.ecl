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

person := dataset('person', { unsigned8 person_id }, thor);

string x1:='a';
string y1:='b';

x1=y1; //Produces TRUE when it should produce false.


output(person,{x1=y1}); //Gives the correct FALSE result.

string x:='a';
string y:='b';

perrec := RECORD
BOOLEAN flag;
person;
END;

perrec SetFlag(person L) := TRANSFORM
SELF.flag := x=y;
SELF := L;
END;

perout := PROJECT(person, SetFlag(LEFT));

output(perout); //Gives incorrect result of TRUE;

