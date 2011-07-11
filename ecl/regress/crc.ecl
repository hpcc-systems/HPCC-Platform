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

export DataLib := SERVICE
  string    PreferredFirst(const string scr)  : c, pure, entrypoint='dataCalcPreferredFirst'; 
END;

qstring20 lnameField := '' : stored('lnameField');
qstring20 fnameField := '' : stored('fnameField');

#uniquename(tf) // true first
%tf%(STRING fname) := DataLib.PreferredFirst(fname);

#uniquename(MatchingDistribute)
x := HASH((%tf%(fnameField))) : persist('gchtestb2');

output(x);
