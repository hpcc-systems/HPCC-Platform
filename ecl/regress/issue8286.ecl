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

idRecord := { unsigned id; };

r := RECORD
unsigned id;
DATASET(idRecord) ids;
END;

r mkR(unsigned i) := TRANSFORM
   temp := '<row><id>' + (string)i + '</id><ids><Row><id>100</id></Row><Row><id>' + (string)(i + 1000) + '</id></Row></ids></row>';
   SELF := FROMXML(r, temp);
END;
 
d := DATASET(20000000, mkR(COUNTER));
output(COUNT(d));
