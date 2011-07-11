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

// test join without transform

r1 := RECORD
  string20 surname := '?????????????';
  string10 forename := '?????????????';
  integer2 age := 25;
END;

r2 := RECORD
  string30 addr := 'Unknown';
  string20 surname;
END;

JoinRecord := RECORD
  string20 surname := '?????????????';
  string10 forename := '?????????????';
  integer2 age := 25;
  string30 addr := 'Unknown';
  // dup:  string20 surname;
END;

namesTable := dataset([{'Salter','Abi',10}], r1);
addressTable := dataset([{'Halliday','10 Slapdash Lane'}], r2);

JoinRecord tranx(r1 l, r2 r) := TRANSFORM
  SELF := l;
  SELF := r;
END;

join_with_trans := join (namesTable, addressTable, 
  LEFT.surname[1..10] = RIGHT.surname[1..10] AND 
  LEFT.surname[11..16] = RIGHT.surname[11..16] AND
  LEFT.forename[1] <> RIGHT.addr[1],
  tranx(LEFT,RIGHT),LEFT RIGHT OUTER);

output(join_with_trans,,'out.d00');

