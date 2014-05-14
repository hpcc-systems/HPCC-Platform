/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems.

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

import javascript;

unicode add5(unicode val) := EMBED(javascript, (varunicode)U' val+\' at Oh là là Straße\';');

add5(U'Hi');


x := dataset('x', { unsigned id; }, csv(terminator((varunicode)'abc')));
output(x);

OUTPUT(UNICODEORDER(U'abcdef', U'abcxyz'));
OUTPUT(UNICODEORDER((varunicode)U'abcdef', (varunicode)U'abcxyz'));
