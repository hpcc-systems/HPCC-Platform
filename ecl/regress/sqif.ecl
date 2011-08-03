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
