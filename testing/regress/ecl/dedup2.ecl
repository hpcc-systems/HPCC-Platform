/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

d := dataset([{1, '1a'},{1, '1b'},{2, '2a'},{2, '2b'},{2, '2c'},{3, '3a'},{4, '4a'},{4, '4b'}],{ unsigned g, string20 l});

unsigned1 dd := 2 : stored('varname');

dde := dedup(d,g,keep(dd));

output(dde)
