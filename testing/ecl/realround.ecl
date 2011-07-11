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




export checkReals(real pi, string version) := FUNCTION
 
    r1 := round(pi/0.1)*0.1;
    r2 := round(pi*10)/10;

    RETURN PARALLEL(       
      output(r1=r2, named('r1_eq_r2'+version)),
      output((string)r1, named('r1'+version)),
      output((string)r2, named('r2'+version)),
       
      output(r1<=3.1, named('r1_le_3_1'+version)),
      output(r2<=3.1, named('r2_le_3_1'+version)),
      output(r1=3.1, named('r1_eq_3_1'+version)),
      output(r2=3.1, named('r2_eq_3_1'+version))
      );
END;

DECIMAL20_10 dec_const := 3.1415926535D;
REAL    real_const := 3.1415926535;
STRING  string_const := '3.1415926535';

DECIMAL20_10 dec_stored := dec_const : STORED('dec_stored');
REAL    real_stored := real_const : STORED('real_stored');
STRING  string_stored := string_const : STORED('string_stored');

checkReals(real_const, '_real_const');
checkReals(dec_const, '_dec_const');
checkReals((real)string_const, '_string_const');

checkReals(real_stored, '_real_stored');
checkReals(dec_stored, '_dec_stored');
checkReals((real)string_stored, '_string_stored');
