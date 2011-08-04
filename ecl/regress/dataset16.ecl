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

import dt;

namesRecord :=
            RECORD,maxlength(999)
string      surname;
unsigned4   len;
dt.vstring(self.len)    forename;
            END;


rec := record, maxlength(1234)
unsigned4       id;
namesRecord     x;
        end;

namesTable := dataset('x',rec,FLAT);

rec t(namesTable l) := transform
    self.x.surname := trim(l.x.surname);
    self := l;
    end;

output(project(namesTable, t(LEFT)));

