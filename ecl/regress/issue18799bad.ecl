/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

crec := RECORD
  string str1;
  string str2;
END;

ds1 := RECORD
  string str {DEFAULT('default1')};
  DATASET(crec) kids {DEFAULT(DATASET([{'str1','str2'}], crec))};
END;

OUTPUT (DATASET([{'test1'}], ds1));
