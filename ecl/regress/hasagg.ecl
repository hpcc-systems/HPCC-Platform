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

person := dataset('person', { unsigned8 person_id, string1 per_sex, string1 per_marital_status, string10 per_first_name, string10 per_last_name }, thor);

r1 :=
     record
        integer8 person_id := person.person_id;
        boolean per_marital_status := person.per_marital_status = 'M';
        boolean per_sex := person.per_sex = 'A';
        boolean per_suffix := person.per_last_name= 'ZZ';
     end;

tx := table(person, r1);
t1 := tx(per_marital_status);

tmpR :=
    record
        r1;
        integer4 seqNum := 0;
    end;

tmpR tmpTrans(tmpR L, tmpR R) :=
    transform    
        self.seqNum := r.seqNum + 1;
        self := l;
    end;

t3 := table(t1, tmpR);
t4 := iterate(t3, tmpTrans(left, right));
 
r2 := record
person_id := t4.person_id;
per_marital_status := t4.per_marital_status;
per_sex := t4.per_sex;
per_suffix := t4.per_suffix;
    end;
 
t5 := table(t4(seqNum <= 20), r2);
 
t6 := table(t4(seqNum > 20), r2);
 
t7 := t5 + t6(per_sex or per_suffix);
 
count(t7);

