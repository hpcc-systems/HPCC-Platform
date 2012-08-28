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

//UseStandardFiles
//UseIndexes

string s1 := 'a' : stored('s1');
string s2 := 'a' : stored('s2');
string s3 := 'a' : stored('s3');
string s4 := 'a' : stored('s4');
string s5 := 'a' : stored('s5');
string s6 := 'a' : stored('s6');
string s7 := 'a' : stored('s7');
string s8 := 'a' : stored('s8');

boolean fuzzy(string a, string b) := ((integer) a[1] * (integer) b[1] != 10);

i := DG_FetchIndex1(WILD(Fname),KEYED(Lname IN ['Anderson', 'Smith']));
i1 := DG_FetchIndex1(WILD(Fname),KEYED(Lname IN ['Anderson', 'Smith']),fuzzy(Fname, s1));
i2 := DG_FetchIndex1(WILD(Fname),KEYED(Lname IN ['Anderson', 'Smith']),fuzzy(Fname, s2));
i3 := DG_FetchIndex1(WILD(Fname),KEYED(Lname IN ['Anderson', 'Smith']),fuzzy(Fname, s3));
i4 := DG_FetchIndex1(WILD(Fname),KEYED(Lname IN ['Anderson', 'Smith']),fuzzy(Fname, s4));
i5 := DG_FetchIndex1(WILD(Fname),KEYED(Lname IN ['Anderson', 'Smith']),fuzzy(Fname, s5));
i6 := DG_FetchIndex1(WILD(Fname),KEYED(Lname IN ['Anderson', 'Smith']),fuzzy(Fname, s6));
i7 := DG_FetchIndex1(WILD(Fname),KEYED(Lname IN ['Anderson', 'Smith']),fuzzy(Fname, s7));
i8 := DG_FetchIndex1(WILD(Fname),KEYED(Lname IN ['Anderson', 'Smith']),fuzzy(Fname, s8));


output(sort(i, lname, fname), {fname, lname});
output('LIMIT, keyed, hit');
output(limit(i, 0, keyed, count, SKIP), {fname, lname});
output(limit(i, 1, keyed, count, SKIP), {fname, lname});
output(limit(i, 5, keyed, count, SKIP), {fname, lname});
output(limit(i, 6, keyed, count, SKIP), {fname, lname});
output(limit(i, 9, keyed, count, SKIP), {fname, lname});
output('LIMIT, keyed, not hit');
output(sort(limit(i, 10, keyed, count, SKIP), lname, fname), {fname, lname});
output(sort(limit(i, 11, keyed, count, SKIP), lname, fname), {fname, lname});
output(sort(limit(i, 20, keyed, count, SKIP), lname, fname), {fname, lname});
output('LIMIT, not keyed, hit');
output(limit(i1, 0, SKIP), {fname, lname});
output(limit(i2, 1, SKIP), {fname, lname});
output(limit(i3, 5, SKIP), {fname, lname});
output(limit(i4, 6, SKIP), {fname, lname});
output(limit(i5, 9, SKIP), {fname, lname});
output('LIMIT, not keyed, not hit');
output(sort(limit(i6, 10, SKIP), lname, fname), {fname, lname});
output(sort(limit(i7, 11, SKIP), lname, fname), {fname, lname});
output(sort(limit(i8, 20, SKIP), lname, fname), {fname, lname});
output('LIMIT, keyed, false condition');
two := 2 : STORED('two');
output(limit(i(1 = two), 9, keyed, count, SKIP), {fname, lname});
