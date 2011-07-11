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

didRec := record
unsigned6   did;
        end;


namesRecord := 
            RECORD
unsigned        jobid;
dataset(didRec) didin;
dataset(didRec) didout;
            END;


func(dataset(didRec) didin) := function

ds0 := dedup(didin, did, all);
ds1 := __COMMON__(ds0);
ds := ds0;
f1 := ds(did != 0);
f2 := ds(did = 0);

c := nofold(f1 + f2);

return sort(c, did);
end;


namesTable := dataset('x',namesRecord,FLAT);


namesRecord t(namesRecord l) := transform

    self.didout := func(l.didin);
    self := l;
    end;


output(project(namesTable, t(LEFT)));
