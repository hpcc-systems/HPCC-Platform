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

