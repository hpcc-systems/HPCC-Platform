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

rec :=
  record
    string2 state;
  end;


string searchstate := 'FL     ': stored('searchState');

output(dataset([{'FL'}], rec),,'regress::stringkey', OVERWRITE);
rawfile := dataset('regress::stringkey', rec, THOR, preload);

//filtered := rawfile(keyed(state in [searchstate[1..2]]));
filtered := rawfile(keyed(state = searchstate[1..2]));

output(filtered);


set of string searchstate2 := ['FL'] : stored('searchState2');

filtered2 := rawfile(keyed(state in searchstate2));

//output(filtered2);
