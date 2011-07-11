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

gavin := service
    unsigned4 FoldString(const string src) : eclrtl,library='dab',entrypoint='rtlStringFilter';
end;

simpleRecord := RECORD
unsigned4   person_id;
data10      per_surname;
    END;


string mkstring(data x) := (string)x;

simpleDataset := DATASET('test',simpleRecord,FLAT);


output(simpleDataset, {gavin.FoldString((string)per_surname)}, 'out.d00');
output(simpleDataset, {gavin.FoldString(mkstring(per_surname))}, 'out.d00');
