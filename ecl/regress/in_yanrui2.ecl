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

#option ('globalFold', false);
varstring vs1 := (varstring)('ab'+'cde');
varstring vs2 := (varstring)('cde' + 'ef');

// Will be "No" because 'abcd' in the set will be promoted to varstring, while 'abcd' outside is
// String, so the types don't match.
s := if('abcd' in [vs1, 'abcd'], 'Yes', 'No');
s;
