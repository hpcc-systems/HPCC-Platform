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

//Check a single child records can be treated as a blob

import dt;

parentRecord := 
                RECORD
unsigned8           id;
string20            address1;
string20            address2;
string20            address3;
unsigned2           numPeople := ((integer4)0);
dt.ebcdic_pstring           extra;
string10            postcode;
                END;

parentDataset := DATASET('test',parentRecord,FLAT);

filtered := parentDataset(extra IN ['A','A','A','C','C','B','A','D']);

output(filtered,,'out.d00');
