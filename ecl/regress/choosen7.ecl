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
rec := { string name };

ds := group(dataset(['Gavin','Nigel'], rec), name);

chooseEmpty := true : stored('chooseEmpty');


x := if(chooseEmpty, ds(false), ds);

y := choosen(nofold(x), 10);

z1 := table(y, { count(group) });
output(z1);

z2 := table(x, { count(group) });
output(z2);

xx := if(not chooseEmpty, ds(false), ds);

yy := choosen(nofold(xx), 10);

zz1 := table(yy, { count(group) });
output(zz1);

zz2 := table(xx, { count(group) });
output(zz2);
