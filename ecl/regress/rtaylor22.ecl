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

LOADXML('<xml/>');

NamesRecord := record
string10 first;
string20 last;
        end;
r := RECORD
  unsigned integer4 dg_parentid;
  string10 dg_firstname;
  string dg_lastname;
  unsigned integer1 dg_prange;
  IFBLOCK(SELF.dg_prange % 2 = 0)
   string20 extrafield;
  END;
  NamesRecord nm;
  dataset(NamesRecord) names;
 END;

ds := dataset('~RTTEST::OUT::ds', r, thor);


sizeof(ds.nm);
//output(ds.nm);

