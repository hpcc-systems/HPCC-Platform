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

set of integer2 setOfSmall := [] : stored('setOfSmall');
set of integer4 setOfMid := [] :  stored('setOfMid');

boolean useSmall := false : stored('useSmall');

set of integer4 stored4 := setOfSmall : stored('stored4');

set of integer stored8 := if(useSmall, setOfSmall, setOfMid) : stored('stored8');

output(99 in stored4);
output(99 in stored8);



z := record
set of integer8 x := stored4;
     end;

ds := dataset([{stored4}], z);
output(ds);
