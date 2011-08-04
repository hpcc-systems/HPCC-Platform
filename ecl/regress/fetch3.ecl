/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

