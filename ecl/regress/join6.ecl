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

#option ('allJoinGeneratesCompare', true);

namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        age;
            END;

addressRecord :=
            RECORD
string30        addr;
string20        surname;
            END;

namesTable := dataset('name', namesRecord, thor);

addressTable := dataset('addr', addressRecord, thor);

JoinRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
string30        addr;
            END;

JoinRecord JoinTransform (namesRecord l, addressRecord r) := 
                TRANSFORM
                    SELF.addr := r.addr;
                    SELF := l;
                END;

JoinedF := join (namesTable, addressTable, 
                LEFT.surname[1..10] = RIGHT.surname[1..10] AND 
                LEFT.surname[11..16] = RIGHT.surname[11..16] AND
                LEFT.forename[1] <> RIGHT.addr[1],
                JoinTransform (LEFT, RIGHT), LEFT OUTER, all);


output(JoinedF,,'out.d00');

