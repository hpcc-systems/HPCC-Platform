/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    This program is free software: you can redistribute it and/or modify
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

#option ('importAllModules', true);

namesRecord := 
            RECORD
string20        surname;
string10        forename;
            END;

entryRecord := RECORD
unsigned4       id;
namesRecord     name1;
namesRecord     name2;
            END;

inTable := dataset('x',entryRecord,THOR);

p := project(inTable, transform(namesRecord, SELF := IF(LEFT.id > 100, LEFT.name1, LEFT.name2)));

output(p);
