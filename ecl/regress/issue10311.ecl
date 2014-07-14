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

Rec := RECORD
  UNSIGNED8 rid;
  REAL fat;
  REAL sugars;
  REAL rating;
 END;

ds := DATASET([{1,1,6,68.402973},{2,5,8,33.983679},{3,1,5,59.425505},{4,0,0,93.704912}],Rec);
dsk:=INDEX(ds,{(UNSIGNED6)RID},{ds},'~key::test');
BUILDINDEX(dsk,OVERWRITE);

dsk2:=INDEX(ds,{UNSIGNED6 RID:=RID},{ds},'~key::test');
BUILDINDEX(dsk2,OVERWRITE);
