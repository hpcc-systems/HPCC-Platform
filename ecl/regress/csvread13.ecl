/*##############################################################################

    Copyright (C) <2010>  <LexisNexis Risk Data Management Inc.>

    All rights reserved. This program is NOT PRESENTLY free software: you can NOT redistribute it and/or modify
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
string      field1;
string      field2;
string      field3;
string      field4;
string      field5;
            END;

allRecord :=
            RECORD
string      field;
       END;

allRecord t(accountRecord l) := TRANSFORM
    SELF.field :=  l.field1 + '*' + l.field2 + '*' + l.field3 + '*' + l.field4 + '*' + l.field5 + TRANSFER(l, STRING10);
END;

inputTable := dataset('~file::127.0.0.1::temp::csvread.csv',accountRecord,CSV(HEADING(1),SEPARATOR([',','==>']),QUOTE(['\'','"','$$']),TERMINATOR(['\r\n','\r','\n'])));
output(PROJECT(inputTable,t(LEFT),KEYED));
