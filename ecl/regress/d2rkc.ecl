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

// supplied
legacy_file := dataset('legacy', { string9 cid; integer8 attr1; }, FLAT);

//generated
person := dataset('person', { unsigned8 person_id }, thor);
hole_result := TABLE(person,{ string9 cid := ''; integer8 atr1 := 0; });

join_rec :=
    RECORD
        string9 cid;
        integer8 oldval;
        integer8 newval;
    END;

join_rec join_em(hole_result l, legacy_file r) :=
    TRANSFORM
        self.cid := l.cid;
        self.newval := l.atr1;
        self.oldval := r.attr1;
    END;

joinedx := join(hole_result, legacy_file, left.cid=right.cid, join_em(LEFT,RIGHT));

output(joinedx);