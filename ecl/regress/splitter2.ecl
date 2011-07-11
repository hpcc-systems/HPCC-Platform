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

#option ('targetClusterType', 'roxie');

namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

//balanced split
t1 := dataset('x',namesRecord,FLAT);
s := sort(t1, surname);
c1 := s + s;
output(c1);


//unbalanced split
t2 := dataset('x2',namesRecord,FLAT);
s2 := sort(t2, surname);
c2 := s2 & s2;
output(C2);


//unbalanced split
t3 := dataset('x3',namesRecord,FLAT);
s3 := sort(t3, surname);
c3 := merge(s3, s3);
output(c3);


//balanced thor, unbalanced roxie, takes a bit of doing to force it!
t4 := dataset('x4',namesRecord,FLAT);
s4a := dedup(t4, forename);
d4b := distribute(s4a, hash(surname));
d4c := distribute(s4a, hash(forename));
s4d := sort(d4b, surname, local);
c4e := s4d & d4c;
d4f := distribute(c4e, hash(age));
s4g := sort(d4f, forename, local);
d4h := distribute(nofold(s4g), hash(surname, forename));
output(d4h);


//balanced thor, unbalanced roxie, follow the dependency..
t5 := dataset('x5', namesRecord, flat);
s5 := dedup(t5, surname);

cnt := count(s5(surname <> 'xx'));
aveAge := ave(s5(surname <> 'yy'), age);

c5 := s5(if(cnt > 100, (forename = 'John'), (age != aveAge)));
output(c5);
