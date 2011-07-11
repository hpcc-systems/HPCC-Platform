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

  r1 := record
  string f1{maxlength(99)}
    end;

r2 := record
  r1;
  unsigned4 id;
  end;


t1 := dataset('d1', r1, thor);
output(t1);

t2 := dataset('d2', r2, thor);
output(t2);

r3 := record
    t2.f1;
    f1a := t2.f1;
    f1b{maxlength(10)} := t2.f1;
    end;
      
t3 := dataset('d3', r3, thor);
output(t3);

