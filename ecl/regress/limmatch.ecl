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

recL := record
  VARSTRING name {MAXLENGTH(11)};
  unsigned  val;
END;
  
recR := record
  VARSTRING name {MAXLENGTH(12)};
  unsigned2     val;
END;

recO := record
  STRING10  lname;
  STRING11  rname;
END;


dsL := DATASET([
  {'BABAK',1},
  {'BABY',2},
  {'BARBARB',3},
  {'BEBO',4}
], recL);
  
dsR := DATASET([
  {'BABARA',5},
  {'BABATUNDE',6},
  {'BABETTE',7},
  {'BABS',8},
  {'BARBAR',9},
  {'BARBARBA',10},
  {'BARBARBO',11},
  {'BARBER',12},
  {'BETTY',13}
], recR);

recO T(recL l,recR r) := TRANSFORM
  self.lname := l.name;
  self.rname := r.name;
END;

  
J1 := JOIN(dsL,dsR,left.name[1..*]=right.name[3..*] and left.val<right.val,T(LEFT,RIGHT), ATMOST(left.name[1..*]=right.name[3..*],3));

output(J1);


