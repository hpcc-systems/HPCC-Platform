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


