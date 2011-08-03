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
output(table(sqNamesTable1, { sum(group, aage),exists(group),exists(group,aage>0),exists(group,aage>100),count(group,aage>20) }, keyed));

//Filtered disk aggregate, which also requires a beenProcessed flag
output(table(sqNamesTable2(surname != 'Hawthorn'), { max(group, aage) }, keyed));

//Special case count.
output(table(sqNamesTable3(forename = 'Gavin'), { count(group) }, keyed));

count(sqNamesTable4, keyed);

//Special case count.
output(table(sqNamesTable5, { count(group, (forename = 'Gavin')) }));

//existance checks
exists(sqNamesTable4, keyed);
exists(sqNamesTable4(forename = 'Gavin'), keyed);
exists(sqNamesTable4(forename = 'Joshua'), keyed);
