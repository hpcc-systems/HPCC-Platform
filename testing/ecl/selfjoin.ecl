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


namesRecord := 
            RECORD
string20        surname := '?????????????';
string10        forename := '?????????????';
integer2        age := 25;
            END;

namesTable := dataset([
        {'Salter','Abi',10},
        {'Halliday','Gavin',31},
        {'Halliday','Liz',30},
        {'Dear','Jo'},
        {'Dear','Matthew'},
        {'X','Z'}], namesRecord);

JoinRecord := 
            RECORD
namesRecord;
string20        otherforename;
            END;

JoinRecord JoinTransform (namesRecord l, namesRecord r) := 
                TRANSFORM
                    SELF.otherforename := r.forename;
                    SELF := l;
                END;

//This should be spotted as a join to self
Joined1 := join (namesTable, namesTable, LEFT.surname = RIGHT.surname, JoinTransform(LEFT, RIGHT));
output(Joined1);

//This should not be spotted as a self join.
Joined2 := join (namesTable, namesTable, LEFT.forename = RIGHT.surname, JoinTransform(LEFT, RIGHT));
output(joined2);