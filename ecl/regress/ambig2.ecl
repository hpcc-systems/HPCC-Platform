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

didRecord := record
unsigned8       did;
            end;

rec := record
unsigned8       did;
dataset(didRecord) x;
unsigned8       filepos{virtual(fileposition)};
        end;


ds := dataset('ds', rec, thor);


rec t(rec l) := transform
    unsigned6 did := (unsigned6)l.did;
z := dataset([{did},{did}], didRecord);
    grz := group(z, row);
    SELF.x := group(sort(dedup(grz, did),did));
    SELF := l;
    end;


p := project(ds, t(left));


output(p);
