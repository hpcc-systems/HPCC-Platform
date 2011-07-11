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

#option ('globalFold', false);
stTrue := true : stored('stTrue');
stFalse := false : stored('stFalse');
ds := dataset('ds',{string10 first_name1; string10 last_name1; }, flat);
dsx := dataset('dsx',{string10 first_name2; string10 last_name2; }, flat);

ct1 := count(if(stTrue, ds, dsx));
ct2 := count(if(stFalse, ds, dsx));

ct := ct1+ct2;

ct;
