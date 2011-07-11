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

export dstring(string del) := TYPE
    export integer physicallength(string s) := StringLib.StringUnboundedUnsafeFind(s,del)+length(del)-1;
    export string load(string s) := s[1..StringLib.StringUnboundedUnsafeFind(s,del)-1];
    export string store(string s) := s+del; // Untested (vlength output generally broken)
END;

accountRecord := 
            RECORD
dstring(',')    field1;
dstring(',')    field2;
dstring(',')    field3;
dstring(',')    field4;
dstring('\r\n') field5;
            END;

inputTable := dataset('~gavin::input',accountRecord,THOR);
output(inputTable,{(string20)field1,(string4)LENGTH((string)field2),(string20)field2,(string20)field3,(string20)field4,(string20)field5,'\r\n'},'gavin::output');
