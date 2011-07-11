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

export abc := '';
export mac_hash_slimsort(arg1,arg2='abc') := macro

hash64(
trim((string)arg1)+ trim((string)arg2)
)
endmacro;

a := MAC_Hash_Slimsort('fred');
output(a);


export mac_hash_slimsort2(arg1,arg2='\'\'') := macro

hash64(
trim((string)arg1)+ trim((string)arg2)
)
endmacro;

a := MAC_Hash_Slimsort2('jim');

output(a);



export mac_hash_slimsort3(arg1,arg2='\'<' + '>\'') := macro

hash64(
trim((string)arg1)+ trim((string)arg2)
)
endmacro;

a := MAC_Hash_Slimsort3('jim');

output(a);


string defaultSlimSortValue := '\'' + '<default>' + '\'';
export mac_hash_slimsort4(arg1,arg2=defaultSlimSortValue) := macro

hash64(
trim((string)arg1)+ trim((string)arg2)
)
endmacro;

a := MAC_Hash_Slimsort4('jim');

output(a);


