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

#option ('targetClusterType', 'roxie');
//This should be legal, but we need to revisit the way items are serialied to the keyed join slave to handle it.

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset('x',namesRecord,FLAT);



i := index({ string20 surname, unsigned8 id }, 'i');

groupedRecord := record
    string20 surname;
    dataset(recordof(i)) people{maxcount(90)};
end;


gr := group(namesTable, surname, all);

results(dataset(namesRecord) l) := function
    slimmed := table(l, {surname});
    return join(slimmed,i,RIGHT.surname IN SET(l(surname != LEFT.surname), surname), transform(right));
end;

r := rollup(gr, group, transform(groupedRecord, self.people := results(rows(left)); self := left));
output(r);
