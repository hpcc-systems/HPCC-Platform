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



nameRecord :=
        RECORD
string20    forename;
string20    surname;
unsigned4   age;
        END;


inRecord :=
        RECORD
nameRecord  boss;
nameRecord  employee;
        END;


inTable := dataset([
            {'Robert','Zimmerman',50,'Gavin','Hawthorn',33},
            {'Elizabeth','Queen',75,'Tony','Blair',50}], inRecord);

output(inTable);
