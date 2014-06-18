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
LOADXML('<xml/>');

show(value) := macro
    output((string) value + ' =\n');
    output('   round     = ' + (string)round(value) + '\n');
    output('   round(0)  = ' + (string)round(value, 0) + '\n');
    output('   round(1)  = ' + (string)round(value, 1) + '\n');
    output('   round(10) = ' + (string)round(value, 10) + '\n');
    output('   round(32) = ' + (string)round(value, 32) + '\n');
    output('   round(100) = ' + (string)round(value, 100) + '\n');
    output('   round(-1) = ' + (string)round(value, -1) + '\n');
    output('   round(-10) = ' + (string)round(value, -10) + '\n');
    output('   round(-100)= ' + (string)round(value, -100) + '\n');
    output('----\n');
endmacro;

show(0.0D);
show(1.0D);
show(2.1D);
show(2.4999D);
show(2.5000D);
show(2.05000D);
show(5D);
show(1230000D);
show(0.100000D);
show(1.99999999995D);
show(1234567890123456789.0D);
show(0.1234567890123456789D);
show(0.00000000000000000000123456789018);
show(-1234567.890123456789D);
show(12345678901234567890000000000000D);

show(0.0);
show(1.0);
show(2.1);
show(2.4999);
show(2.5000);
show(2.05000);
show(5.0);
show(1230000.0);
show(0.100000);
show(1.99999999995);
show(1234567890123456789.0);
show(0.1234567890123456789);
show(0.000000000000000000001234567890123456789);
show(-1234567.890123456789);
show(1234567890123456789000000000000000.0);
show(1.0e100);
show(1.0e-100);
show(1230000);

showRounded(value, minPos, maxPos) := MACRO
#SET(i,minPos)
#LOOP
  #IF (%i%>maxpos)
    #BREAK
  #END
  output((string)%i% + ' -> ' + (string)round(value,%i%) + '\n');
  #SET(i,%i%+1)
#END
ENDMACRO;


#DECLARE(i)
showRounded(123456789.0, -10, 0);
showRounded(1234567890123456789.0, -28, -1);
showRounded(1234567890123456789.0e50, -70, -1);
