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

evaluate(counts[1],counts.NAge);
counts[1].NAge;   // Re release
evaluate(counts[1],counts.NAge2);
evaluate(counts[1],counts.NInc);
evaluate(counts[1],counts.NInc2+1);
count_inc;
//count_age;
// ---------------------------------------------------

