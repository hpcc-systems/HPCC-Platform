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

// The following should have different crcs for the persists...

d1 := dataset([{'-'}, {'--'}, {'nodashes'}], {string s});

t1 := d1(TRIM(StringLib.StringFilterOut(s, '-')) != '') : persist('maltemp::delete1');


output(t1);


d2 := dataset([{'-'}, {'--'}, {'nodashes'}], {string s});

t2 := d2(TRIM(StringLib.StringFilter(s, '-')) != '') : persist('maltemp::delete2');


output(t2);


