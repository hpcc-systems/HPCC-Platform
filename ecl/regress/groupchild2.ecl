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

#option ('targetClusterType', 'hthor');
#option ('groupedChildIterators', true);
#option ('testLCR', true);
#option ('mainRowsAreLinkCounted', false);

childRecord := RECORD
string      name;
unsigned    dups := 0;
end;


namesRecord := 
            RECORD
string20        surname;
dataset(childRecord) children;
            END;


ds := dataset(
[
    {'Gavin', 
        [{'Smith'},{'Jones'},{'Jones'},{'Doe'},{'Smith'}]
    },
    {'John',
        [{'Bib'},{'Bob'}]
    }
], namesRecord);

namesRecord t(namesRecord l) := transform
    deduped := dedup(group(l.children, name), name);
    cnt(string search) := count(table(deduped(name != search), {count(group)}));
    self.children := group(project(deduped, transform(childRecord, self.dups := cnt(left.name); self.name := left.name)));
    self := l;
end;


output(project(nofold(ds), t(left)));



