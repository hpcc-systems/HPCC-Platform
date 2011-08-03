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

rec := record
 string10 lname;
 string10 fname;
end;

head := dataset('header',rec,flat);

typeof(head) keep_newest(rec l, rec r) := transform
    self.lname := evaluate(if(true, l,r), lname);
    self.fname := evaluate(if(l.lname='gavin', l,r), fname);
end;

new_head := rollup(head, true, keep_newest(left, right));


// THIS WILL NOT SYNTAX CHECK

typeof(head) keep_newestx(rec l, rec r) := transform
 self.lname := if(l.lname='gavin', l, r).lname;
 self := if(l.lname='richard', l, r);
end;

new_headx := rollup(head, true, keep_newestx(left, right));

typeof(head) keep_newesty(rec l, rec r) := transform
 self.lname := if(l.lname='gavin', l, r).lname;
 self.fname := if(l.lname='gavin', l, r).fname;
end;

new_heady := rollup(head, true, keep_newesty(left, right));

output(new_head);
output(new_headx);
output(new_heady);

