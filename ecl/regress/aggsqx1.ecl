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

forceSubQuery(a) := macro
    { dedup(a+a,true)[1] } 
endmacro;

trueValue := true : stored('trueValue');
falseValue := true : stored('falseValue');

persons := sqHousePersonBookDs.persons;

//Simple disk aggregate
a1 := table(persons, { firstForename := (string20)forename, sum(group, aage),exists(group),exists(group,aage>0),exists(group,aage>100),count(group,aage>20) });
output(sqHousePersonBookDs, forceSubQuery(a1((firstForename='zzzzzzz') = falseValue)));

//Filtered disk aggregate, which also requires a beenProcessed flag
a2 := table(persons(surname != 'Halliday'), { max(group, aage) });
output(sqHousePersonBookDs, forceSubQuery(a2));

//Special case count.
a3 := table(persons(forename = 'Gavin'), { count(group) });
output(sqHousePersonBookDs, forceSubQuery(a3));

//Special case count.
a4 := table(persons, { count(group, (forename = 'Gavin')) });
output(sqHousePersonBookDs, forceSubQuery(a4));
