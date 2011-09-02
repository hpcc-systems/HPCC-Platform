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
