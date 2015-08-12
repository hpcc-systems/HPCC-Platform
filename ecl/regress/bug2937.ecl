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

