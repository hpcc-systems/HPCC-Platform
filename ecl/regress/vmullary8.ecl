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

layout_dids := RECORD
  unsigned6 dids;
END;

layout_output := RECORD
  unsigned1 code := 0;
  string64  message:= '';
  DATASET (layout_dids) verified {MAXCOUNT(1)};
  DATASET (layout_dids) id {MAXCOUNT(1)};
END;

ds := DATASET ([{1, 'aaa', [{5}, {6}], []}, {2, 'bbb', [{7}, {8}], []}], layout_output);
OUTPUT (ds, NAMED ('ds'));