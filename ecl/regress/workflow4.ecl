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

#option('workflow', 1);
#option('generateLogicalGraph', true);

layout_hi := RECORD
    STRING20 a;
    UNSIGNED b;
END;

ds := DATASET([{'hi', 0}, {'there', 1}], layout_hi);

layout_hi Copy(ds l) := TRANSFORM
    SELF := l;
END;

norm := NORMALIZE(ds, 2, Copy(LEFT));
norm2 := norm  : SUCCESS(OUTPUT(norm,, 'adtemp::wf_test', OVERWRITE));
OUTPUT(COUNT(norm2));

