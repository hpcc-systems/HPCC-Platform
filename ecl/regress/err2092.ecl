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

aaa := DATASET('aaa', {STRING1 f1, STRING1 f2}, hole);
sorted_aaa := SORT(aaa,f1,f2);
OUTPUT(SORT(aaa, f2, JOINED(sorted_aaa)));

// this is OK although sorted_aaa was sorted on 2 fields.
sorted_aaa2 := SORTED(sorted_aaa,f1);
OUTPUT(SORT(aaa, f2, JOINED(sorted_aaa2)));

bbb := DATASET('bbb', {STRING1 fb}, hole);
sorted_bbb := SORTED(bbb,fb);
OUTPUT(SORT(aaa, f1, f2, JOINED(sorted_bbb)));
