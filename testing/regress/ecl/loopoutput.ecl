/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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


IMPORT Std;
IMPORT ^ as root;
IMPORT $.setup;

prefix := setup.Files(false, false).QueryFilePrefix;

rec := RECORD
 unsigned4 id;
END;

createFunc(DATASET(rec) loopin, unsigned c) := FUNCTION
  ds := DATASET([{c}], rec);
  subName := prefix+'sub' + c;
  o := OUTPUT(ds, , subName, OVERWRITE);
  RETURN WHEN(loopin, o);
END;

readFunc(DATASET(rec) loopin, unsigned c) := FUNCTION
  subName := prefix+'sub' + c;
  ds := DATASET(subName, rec, FLAT);
  RETURN loopin & ds;
END;

numSubFiles := 4;

SEQUENTIAL(
  OUTPUT(LOOP(DATASET([], rec), numSubFiles, createFunc(ROWS(LEFT), COUNTER)));
  OUTPUT(LOOP(DATASET([], rec), numSubFiles, readFunc(ROWS(LEFT), COUNTER)));
);