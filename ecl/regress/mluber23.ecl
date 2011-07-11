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

// The following should have different crcs for the persists...

d1 := dataset([{'-'}, {'--'}, {'nodashes'}], {string s});

t1 := d1(TRIM(StringLib.StringFilterOut(s, '-')) != '') : persist('maltemp::delete1');


output(t1);


d2 := dataset([{'-'}, {'--'}, {'nodashes'}], {string s});

t2 := d2(TRIM(StringLib.StringFilter(s, '-')) != '') : persist('maltemp::delete2');


output(t2);


