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


sequential(
//Normalized, count
output(sqNamesTable1.books(rating100>50), { count(group) }),

//Normalized, aggregate
output(table(sqNamesTable2.books, { max(group, rating100) }, keyed)),

//Normalized, grouped aggregate
output(table(sqNamesTable3.books, { count(group), rating100}, rating100, keyed)),

//Normalized, grouped aggregate - criteria is in parent dataset
output(table(sqNamesTable4.books, { count(group), sqNamesTable4.surname }, sqNamesTable4.surname, keyed)));
