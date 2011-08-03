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