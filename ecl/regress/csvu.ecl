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


namesRecord :=
            RECORD
unicode20       surname;
unicode10       forename;
integer2        age := 25;
            END;

namesTable := dataset('x',namesRecord,FLAT);

namesTable2 := dataset([
        {'Hawthorn',U'Gaéëvin',31},
        {'Hawthorn',U'τετελεσταιι',30},
        {'Smithe','Pru',10},
        {'X','Z'}], namesRecord);

output(namesTable2,,'out1.d00',CSV);

output(namesTable2,,'out2.d00',CSV(MAXLENGTH(9999),SEPARATOR(';'),TERMINATOR(['\r\n','\r','\n','\032']),QUOTE(['\'','"'])));
