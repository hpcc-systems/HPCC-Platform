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

import AggCommon;
AggCommon.CommonDefinitions();

//Simple disk aggregate
output(table(sqNamesIndex1, { sum(group, aage),exists(group),exists(group,aage>0),exists(group,aage>100),count(group,aage>20) }));

//Filtered disk aggregate, which also requires a beenProcessed flag
output(table(sqNamesIndex2(surname != 'Hawthorn'), { max(group, aage) }));

//Special case count.
output(table(sqNamesIndex3(forename = 'Gavin'), { count(group) }));

count(sqNamesIndex4);

//Special case count.
output(table(sqNamesIndex5, { count(group, (forename = 'Gavin')) }));
