/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

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

import Std.Metaphone, lib_metaphone;

input := DATASET([
  {'Algernon'},
  {'Englebert'},
  {'Cholmondley'},
  {'Farquar'}
], { string name});

outrec := RECORD
  STRING name;
  STRING d1;
  STRING d2;
  STRING db;
  STRING20 d1_20;
  STRING20 d2_20;
  STRING40 db_40;
END;

outrec t(string name) := TRANSFORM
   SELF.name := name;
   SELF.d1 := Metaphone.primary(name);
   SELF.d2 := Metaphone.secondary(name);
   SELF.db := Metaphone.double(name);
   SELF.d1_20 := lib_metaphone.MetaphoneLib.DMetaphone1_20(name);
   SELF.d2_20 := lib_metaphone.MetaphoneLib.DMetaphone2_20(name);
   SELF.db_40  := lib_metaphone.MetaphoneLib.DMetaphoneBoth_40(name);
END;

output(PROJECT(input, t(LEFT.name)));