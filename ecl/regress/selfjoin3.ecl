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

namesRecord :=
            RECORD
string20        surname := '?????????????';
string10        forename := '?????????????';
integer2        age := 25;
            END;

namesTable := sort(dataset('namesTable', namesRecord, thor), surname);


namesRecord JoinTransform (namesRecord l, namesRecord r) :=
                TRANSFORM
                    SELF.forename := r.forename;
                    SELF := l;
                END;

j := join (namesTable, namesTable, LEFT.surname = RIGHT.surname, JoinTransform(LEFT, RIGHT), local);
x := sort(j, forename);
output(x,,'out.d00');
