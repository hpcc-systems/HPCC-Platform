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


inrec := record
unsigned6 did;
    end;

outrec := record(inrec)
string20        name;
unsigned        score;
          end;

ds := dataset('ds', inrec, thor);

dsg := group(ds, row);

c1 := soapcall(dsg, 'remote1', 'service', inrec, transform(left), dataset(outrec));
c2 := soapcall(dsg, 'remote2', 'service', inrec, transform(left), dataset(outrec));
c3 := soapcall(dsg, 'remote3', 'service', inrec, transform(left), dataset(outrec));

//perform 3 soap calls in parallel
combined := regroup(c1, c2, c3);

// choose the best 5 results for each input row
best := topn(combined, 5, -score);

output(best);
