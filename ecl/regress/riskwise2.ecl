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
string20        surname;
string10        forename;
string8         dt_last_seen := '';
integer2        age := 25;
            END;

namesTable := dataset([
        {'Hawthorn','Gavin','700117',31},
        {'Hawthorn','Mia','723213', 30},
        {'Smithe','Pru','900120', 10},
        {'X','Z'}], namesRecord);

namesRecord filter1(namesRecord le, namesRecord ri) := transform
    chooser1 := ri.dt_last_seen[1..6] > le.dt_last_seen[1..6];
    self := if(chooser1, ri, le);
end;

one_gong := rollup(namesTable, true, filter1(left, right));                     //do the actual rollup to narrow down to one record

output(one_gong, named('gong'));

x := if (exists(one_gong), one_gong, dedup(one_gong));

output(x);
