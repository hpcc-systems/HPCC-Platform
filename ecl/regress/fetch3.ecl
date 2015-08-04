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

LOADXML('<root><did>1234567</did></root>');

layout_relatives_plus :=
        RECORD
integer8    person1;
boolean     same_lname;
integer8    person2;
string20    surname;
string20    forename;
unsigned8   fpos{virtual(fileposition)};
        END;

File_Relatives_Plus := dataset
('BASE::Relatives1_xx',layout_relatives_plus,flat);

d := File_Relatives_Plus;

Key_Relatives := INDEX(d, {person1,same_lname,person2, fpos}, '~thor::key::relatives');

relas := Key_Relatives(person1=%did%);

output(relas)

