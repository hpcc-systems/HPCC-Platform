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

//Perverse syntax to demonstrate the lack of hard reserved words....
import dt;

integer RETURN := 1234;
unsigned4 FULL := 100;

x := full * RETURN;
output(x);

r := record,maxlength(12345)
    unsigned4 group;
    boolean flat;
    boolean all := false;
    boolean dt;
    string maxlength := '123';
    end;


ds := dataset('ds', r, thor);


ds2 := dedup(ds, ds.group, ds.all, all);

output(ds2, { ds2.group });


t := table(ds2, {unsigned8 group := ds2.group; ds2.flat, ds2.all, ds2.dt; });

output(t,,'out.xml',xml);

output(ds,,'out2.xml',xml);