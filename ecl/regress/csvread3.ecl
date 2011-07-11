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


accountRecord := 
            RECORD
string20        field1;
string20        field2;
                ifblock(self.field1 <> 'empty')
string20            field3;
string10            field4;
                end;
unsigned4       filepos{virtual(fileposition)}
            END;

inputTable := dataset('~.::csvin',accountRecord,CSV);
output(inputTable,{filepos,': >>',field1,field2,field3,field4,'<<'});
