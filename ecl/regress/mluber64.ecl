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

l :=
RECORD
    STRING100 s;
    unsigned docid;
    unsigned wordpos;
END;


ds := DATASET([{'a',1,1},{'a',2,1},{'a',3,1},{'b',2,2},{'b',3,2},{'c',3,3}],l);
ds_sorted := SORT(DISTRIBUTE(ds,HASH(docid)),s,docid,wordpos,LOCAL);


i := INDEX(ds_sorted, {s,docid,wordpos},'~thor::key::localindex');


i_read := i(keyed(s='a'));
local_i_read := allnodes(local(i_read));

output(i);
output(i_read);
output(local_i_read);
