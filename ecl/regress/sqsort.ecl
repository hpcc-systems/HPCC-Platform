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


#option ('targetClusterType', 'hthor');

import * from sq;
DeclareCommon();


myPeople := sqSimplePersonBookDs(surname <> '');

childBookRec :=
            RECORD
dataset(sqBookIdRec)        books{blob};
            END;

slimPeopleRec :=
            RECORD
string20        surname;
childBookRec        child;
            END;

//Perverse coding to ensure we sort by a linked child record - to ensure it is correctly serialised
p := project(myPeople, transform(slimPeopleRec, self.child.books := left.books; self := left));
s := sort(p, -p.child);
output(s);
