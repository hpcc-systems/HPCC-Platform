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

__set_debug_option__('staticResource',1);
__set_debug_option__('optimizeDiskRead', 0);

baseRecord :=
            RECORD
unsigned8       id;
string20        surname;
string30        forename;
unsigned8       filepos{virtual(fileposition)}
            END;

baseTable := DATASET('base', baseRecord, THOR);

filteredTable1 := baseTable(surname <> 'Hawthorn');
filteredTable2 := baseTable(surname <> 'Drimbad');


baseRecord t(baseRecord l) :=
    TRANSFORM
        SELF := l;
    END;

//-------------------------------------------

x := JOIN(filteredTable1, filteredTable2, LEFT.forename = RIGHT.forename, t(LEFT));

output(x,,'out.d00');

