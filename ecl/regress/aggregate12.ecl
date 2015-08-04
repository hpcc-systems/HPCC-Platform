/*##############################################################################

    Copyright (C) 2012 HPCC Systems®.

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

rec := RECORD
   unsigned x;
   unsigned value;
END;

ds := DATASET([{1,1},{2,2},{3,3}], rec); dds := DISTRIBUTE(ds, x) : PERSIST('dds');

a := AGGREGATE(dds, rec,
        TRANSFORM(rec, SELF.value := IF(RIGHT.x<>0, LEFT.Value*RIGHT.Value, LEFT.Value), SELF := LEFT),
         TRANSFORM(rec, SELF.value := RIGHT1.Value*RIGHT2.Value, SELF := RIGHT2));

OUTPUT(a);
