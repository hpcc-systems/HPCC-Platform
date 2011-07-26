/*##############################################################################

    Copyright (C) <2010>  <LexisNexis Risk Data Management Inc.>

    All rights reserved. This program is NOT PRESENTLY free software: you can NOT redistribute it and/or modify
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

makeDataset(set of integer x) := dataset(x, { integer value; });

isUnique1(set of integer x) := not exists(makeDataset(x)(value in x));

isUnique2(set of integer x) := not exists(join(makeDataset(x),makeDataset(x),left.value=right.value,all));

set of integer values := [1,2,3,4,5,6,3,2,7] : stored('values');


output(isUnique1(values));
output(isUnique2(values));
