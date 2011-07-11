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
string30        abc;
integer2        age := 25;
            END;

namesTable := dataset('x', namesRecord, THOR);

x := distribute(namesTable, HASH(surname,forename));

y := group(x, surname, forename,local);


ya1 := dedup(sort(y, abc), abc);
ya2 := table(ya1, {surname, forename, unsigned8 cnt := count(group)}, surname, forename, local);

yb1 := dedup(sort(y, age), age);
yb2 := table(yb1, {surname, forename, unsigned8 cnt := count(group)}, surname, forename, local);

JoinRecord := 
            RECORD
namesTable.surname;
namesTable.forename;
unsigned8       cnt_abc;
unsigned8       cnt_age;
            END;

JoinRecord JoinTransform (ya2 l, yb2 r) := 
                TRANSFORM
                    SELF.cnt_abc := l.cnt;
                    SELF.cnt_age := r.cnt;
                    SELF := l;
                END;

//This should be spotted as a join to self
Joined1 := join (ya2, yb2, LEFT.surname = RIGHT.surname AND LEFT.forename = RIGHT.forename, JoinTransform(LEFT, RIGHT), local) : persist('joined1');
Joined2 := sort(Joined1, forename, local);
output(Joined2,,'out.d00');

