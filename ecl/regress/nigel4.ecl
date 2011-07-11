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


rec := record
     REAL r;
     DECIMAL5 d;
     QSTRING7 q;
     UNICODE9 u;
     VARSTRING v;
       end;

outrec := record
        unsigned8 h;
      end;

ds := DATASET([{1.2,2.2,'333','444','555'}],rec);

output(ds);

outrec T(rec L, rec r) := transform
    self.h := hash64(L.r);
end;

output(iterate(ds,T(LEFT, right)));
