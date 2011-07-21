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

isUnique1(set of integer x) := function
    ds1 := dataset(x, { integer value1; });
    ds2 := dataset(x, { integer value2; });
    return not exists(ds1(count(ds2(ds1.value1 = ds2.value2)) > 1));
END;

isUnique2(set of integer x) := function
    ds1 := dataset(x, { integer value; });
    return count(x) = count(dedup(sort(ds1, value), value));
END;

set of integer values := [1,2,3,4,5,6,7] : stored('values');

output(isUnique1(values));
output(isUnique2(values));
