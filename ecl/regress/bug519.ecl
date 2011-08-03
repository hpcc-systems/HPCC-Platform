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

#option ('globalFold', false);

string isEmpty(set of integer a) := IF ( a = [], 'yes','no' );
string isOneTwoThree(set of integer a) := IF ( a = [1,2,3], 'yes','no' );
string aboveOneTwo(set of integer a) := IF ( a > [1,2], 'yes','no' );
string aboveOneThousandEtc(set of integer1 a) := IF ( a > [1000,1001], 'yes','no' );
string biggerThanMe(set of string a) := IF ( a > ['Gavin','Hawthorn'], 'yes','no' );

isEmpty([1,2]);
isEmpty([]);
isOneTwoThree([1,2]);
isOneTwoThree([1,2,3]);
isOneTwoThree([1,2,3,4]);
aboveOneTwo([1]);
aboveOneTwo([1,2]);
aboveOneTwo([1,2,3]);
aboveOneThousandEtc([1,2,3]);
biggerThanMe(['Richard','Drimbad']);
