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

#option ('optimizeGraph', false);
__set_debug_option__('optimizeDiskRead', 0);

baseRecord :=
            RECORD
unsigned8       id;
string20        surname;
string30        forename;
unsigned8       age;
            END;

//baseTable := DATASET('base', baseRecord, THOR);

integer xxthreshold := 10           : stored('threshold');

baseTable := nofold(dataset([
        {1, 'Hawthorn','Gavin',31},
        {2, 'Hawthorn','Mia',30},
        {3, 'Smithe','David',10},
        {4, 'Smithe','Pru',10},
        {5, 'Stott','Mia',30},
        {6, 'X','Z', 99}], baseRecord));


filteredTable := baseTable(surname <> 'Hawthorn');

//-------------------------------------------

x := filteredTable(id < count(filteredTable));

output(x,,'out.d00',overwrite);
