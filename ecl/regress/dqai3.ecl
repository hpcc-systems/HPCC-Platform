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

f := nofold(dataset([{'20071001'}],{string8 addr_version_number}));

f_1 := f((unsigned)addr_version_number<20070915);
f_2 := f((unsigned)addr_version_number>=20070915,
         (unsigned)addr_version_number<20070920);
f_3 := f((unsigned)addr_version_number>=20070920,
         (unsigned)addr_version_number<20071030);

f_out := if(count(f_1)>0,dataset([{1}],{unsigned1 id})) +
         if(count(f_2)>0,dataset([{2}],{unsigned1 id})) +
         if(count(f_3)>0,dataset([{3}],{unsigned1 id}));

output(f_out);
