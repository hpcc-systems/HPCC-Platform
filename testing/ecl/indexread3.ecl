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
