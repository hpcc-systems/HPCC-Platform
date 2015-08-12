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

#option ('childQueries', true);

import sq;
sq.DeclareCommon();


trueValue := true : stored('true');
string20 searchName := 'Gavin Hawthorn' : stored('SearchName');


lhs := sqHousePersonBookDs;

sqHousePersonBookIdExRec t(sqHousePersonBookIdExRec l) := transform
    self.persons := sort(l.persons, trim(searchName)[id]);
    self := l;
    end;

rhs  := project(sqHousePersonBookDs, t(left));



cond := IF(trueValue, lhs, rhs);

output(cond);
