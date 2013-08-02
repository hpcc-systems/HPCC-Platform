/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.

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

f := false : stored('f');

output((unicode0)U'X' != U'X');
output(U'' + U'' + U'' != U'Y');
output(IF(1 = 2, U'', U'') != U'Z');
output(IF(f, U'', U'') != U'A');

output(U'Xabcdefhehe' != (unicode0)U'X');
output(U'Y' != U'' + U'' + U'');
output(U'Z' != IF(1 = 2, U'', U''));
output(U'A' != IF(f, U'', U''));

ne(unicode x, unicode y) := x != y;

output(ne(U'Xabcdefhehe', (unicode0)U'X'));
output(ne(U'Y', U'' + U'' + U''));
output(ne(U'Z', IF(1 = 2, U'', U'')));
output(ne(U'A', IF(f, U'', U'')));
