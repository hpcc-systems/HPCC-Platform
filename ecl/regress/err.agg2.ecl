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

#option ('pickBestEngine', false);

SHARED friendrec := RECORD
integer inc; 
integer age;
string2 state := 'fl';
END;

SHARED MyFriends := DATASET ([{1000, 30}, {15000, -1}, {-1, 50},
{2500, -1}],  friendrec);

SHARED IfAge := IF (MyFriends.age <> -1, 1, 0);

SHARED IfInc := IF (MyFriends.inc <> -1, 1, 0);

CountRec := RECORD
  NAge := SUM(GROUP, IfAge);
  NAge2 := COUNT(GROUP, MyFriends.age <> -1);
  NInc := SUM(GROUP, IfInc);
  NInc2 := SUM(GROUP, MyFriends.age, MyFriends.inc <> -1);
END;

counts := TABLE(MyFriends, CountRec);

//----------------- End of Common Section ------------------------


//OUTPUT(counts);    // Works fine
// ---------------------------------------------------

//count_inc := counts[1].NAge;   // Does Not Work !!
//count_inc := counts[1].NAge+1;   // Does Not Work !!
//count_inc := evaluate(counts[1],counts.NAge);   // Does Not Work !!
count_inc := evaluate(counts[1],counts.NAge);   // Does Not Work !!
//count_age := counts[1].NInc;   

// this is not allowed
counts[1].NAge;

// this is ok
evaluate(counts[1],counts.NAge);

evaluate(counts[1],counts.NAge2);
evaluate(counts[1],counts.NInc);
evaluate(counts[1],counts.NInc2+1);
count_inc;

//count_age;
// ---------------------------------------------------

