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



ds := dataset([CLUSTERSIZE],{unsigned4 size});

output(ds);

ds2 := ds : persist('savedds2', 'thor800');
output(ds2);

ds3 := ds : persist('savedds3', 'hthor');
output(ds3);

#option ('clusterSize', 300);
