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

layout := {
    unsigned1 id,
    string s1,
};

layout2 := {
    unsigned1 id,
    string s2,
};

ds1 := dataset([{1,'1'},{2,'2'}], layout);
ds2 := dataset([{1,'one'},{2,'two'}], layout2);

ds1_dist := distribute(ds1, id);
ds2_dist := distribute(ds2, id);

// FAIL causes compiler to lose knowledge of distribution
ds1A := if(exists(ds1_dist(s1 = '1')),
           ds1_dist,
           fail(ds1_dist, 'message'));

// The graph shows that this join is NOT local
ds3 := join(ds1A, ds2_dist,
            left.id = right.id);
output(ds3);

// compiler retains knowledge of distribution
ds1B := if(exists(ds1_dist(s1 = '1')),
           ds1_dist,
           ds1_dist(s1 = '2'));

// The graph shows that this join IS local.
ds4 := join(ds1B, ds2_dist,
            left.id = right.id);
output(ds4);
