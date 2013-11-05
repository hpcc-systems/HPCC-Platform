/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
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
