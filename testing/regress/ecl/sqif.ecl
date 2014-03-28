/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

import $.setup.sq;

trueValue := true : stored('true');
subLength := 20 : stored('subLength');

integer searchValue := 1234567890123456789;
string searchName := 'Gavin Halliday' : stored('SearchName');


lhs := sq.HousePersonBookDs;

sq.HousePersonBookIdExRec t(sq.HousePersonBookIdExRec l) := transform
    self.persons := sort(l.persons, TRIM(searchName)[id], INTFORMAT(searchValue, subLength, 20)[id]);
    self := l;
    end;

rhs  := project(sq.HousePersonBookDs, t(left));

cond := IF(trueValue, lhs, rhs);

output(cond);
