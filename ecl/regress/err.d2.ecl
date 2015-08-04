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

Module Compare;


r_legacy    := RECORD
                integer attr1;
                string4 attr2;
                integer8 cid;
               END;

old_legacy_file := DATASET('logical_file_name', r_legacy, FLAT);

r_new       := RECORD
                integer8 cid;
                integer attr1;
                integer attr2;
               END;

r_new conv_leg(r_legacy l) :=
               TRANSFORM
                self.attr2 := (integer8) l.attr2;
                self := l;
               END;

legacy_file := PROJECT(old_legacy_file, conv_leg(LEFT));

// Or hole_result = dataset(...) if I prefer


person := dataset('person', { unsigned8 person_id, string1 per_sex, string10 per_first_name, string10 per_last_name }, thor);
hole_result := TABLE(person,r_new);


Result_type := RECORD
                integer8 cid;
                boolean was_same_attr1;
                boolean was_same_attr2;
               END;

Result_type CompareFunc(r_new l, r_legacy r) :=
               TRANSFORM
                SELF.cid := l.cid;
                SELF.was_same_attr1 := l.attr1 = r.attr1;
                SELF.was_same_attr2 := (string) l.attr2 = r.attr2;
               END;

// Will be used to show 'top level' statistics

TheResult   := JOIN(hole_result, legacy_file, LEFT.cid = RIGHT.cid, CompareFunc(LEFT, RIGHT));

OUTPUT(TheResult);

with_hole   := RECORD
                integer8 cid;
                integer8 attr_hole;
                integer8 attr_legacy;
               END;

with_hole join_hole_val(result_type l, r_new r) :=
               TRANSFORM
                SELF.cid := l.cid;
                SELF.attr_hole := r.attr1;
               END;

x := theresult(was_same_attr1);

CompVals    := JOIN(TheResult(NOT was_same_attr1), hole_result, LEFT.cid = RIGHT.cid, join_hole_val(LEFT, RIGHT));

with_hole join_legacy_val(with_hole l, r_legacy r) :=
               TRANSFORM
                SELF.attr_legacy := r.attr1;
                SELF := l;
               END;

// Shows why a given attribute is broken
CompVals1   := JOIN(CompVals, legacy_file, LEFT.cid = RIGHT.cid0), join_legacy_val(LEFT, RIGHT));

OUTPUT(CompVals1);

count(person);
