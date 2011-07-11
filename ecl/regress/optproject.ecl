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
