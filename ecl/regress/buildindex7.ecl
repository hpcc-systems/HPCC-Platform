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


mainRecord :=
        RECORD
integer8            sequence;
string20            forename;
string20            surname;
unsigned8           filepos{virtual(fileposition)};
        END;

mainTable := dataset('keyed.d00',mainRecord,THOR);

i1 := index(mainTable, { surname, forename, filepos } ,'keyed.name.1', distributed(hash(forename)));
i2 := index(mainTable, { surname, forename, filepos } ,'keyed.name.2', fixed, compressed(first));

BUILDINDEX(i1,overwrite);

BUILDINDEX(i2,'keyed.name.2.new',overwrite, update, expire);
