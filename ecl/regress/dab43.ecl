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

datalibx := service
        unsigned4 NameMatch(const string le_f, const string le_m, const string le_l,
                      const string ri_f,const string ri_m,const string ri_l)  : c, pure, entrypoint='dataNameMatch';
    end;

name_match() := macro
(string)(datalibx.NameMatch
(u1.fname,u1.mname,u1.lname,u2.fname,u2.mname,u2.lname))
endmacro;



r1 :=
            RECORD
qstring20       fname;
qstring20       mname;
qstring20       lname;
            END;

r2 := record
    r1;
    qstring100 res1;
    qstring100 res2;
    end;

t1 := dataset('t1',r1,FLAT);
t2 := dataset('t2',r1,FLAT);

r2 t(r1 u1, r1 u2) := transform
    self.res1 := 'name(' + name_match() + ')';
    self.res2 := 'namex(' + name_match() + ')';
    self := u1;
    end;

j := join(t1, t2, left.fname = right.fname, t(left, right));

output(j);


