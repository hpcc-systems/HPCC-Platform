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

d := dataset('s.csv', { unsigned4 qid, string20 fieldname, string20 condition, string1000 value}, CSV);

d1 := dedup(group(d, qid), fieldname);

summary := record
unsigned4 qid;
unsigned4 fields;
end;

d1 t1(d1 l, d1 r) := transform
SELF.qid := r.qid;
SELF.condition := '';
SELF.fieldname := '';
SELF.value := IF(l.fieldname!='',TRIM(l.value)+','+r.fieldname,r.fieldname);
END;

d2 := rollup(d1, qid, t1(LEFT, RIGHT));

//output(choosen(d2, 1),,'fff', OVERWRITE);

count(choosen(d2, 1));
