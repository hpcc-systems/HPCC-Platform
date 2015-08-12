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

Layout_FormerName := record
 big_endian unsigned integer4 person_id;
 big_endian unsigned integer1 fnm_type;
 string25 fnm_surname;
 string15 fnm_first_name;
 string15 fnm_middle_name;
 string2 fnm_suffix;
end;

FormerName := dataset('former_name', Layout_FormerName, flat);
Output(formername( person_id in[1,10,105,116,117,119,129,135,139,142,146,3,30,46,64,66,7,73,74,75,85,88,9,92,98]));
