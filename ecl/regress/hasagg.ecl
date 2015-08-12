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

