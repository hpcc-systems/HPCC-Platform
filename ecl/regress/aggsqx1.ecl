/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
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
a2 := table(persons(surname != 'Hawthorn'), { max(group, aage) });
output(sqHousePersonBookDs, forceSubQuery(a2));

//Special case count.
a3 := table(persons(forename = 'Gavin'), { count(group) });
output(sqHousePersonBookDs, forceSubQuery(a3));

//Special case count.
a4 := table(persons, { count(group, (forename = 'Gavin')) });
output(sqHousePersonBookDs, forceSubQuery(a4));
