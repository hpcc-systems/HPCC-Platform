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

//skip type==thorlcr TBD
//skip type==hthor TBD
//skip type==roxie TBD
//UseStandardFiles

output(choosen(DG_IntegerIndex, 3));

output(DG_IntegerIndex(keyed(i6 = 4)));

//Filters on nested integer fields work, but range filters will not because the fields
//are not biased.
output(DG_IntegerIndex(wild(i6),keyed(nested.i4 = 5)));
output(DG_IntegerIndex(wild(i6),wild(nested.i4),keyed(nested.u3 = 6)));
output(DG_IntegerIndex(i5 = 7));
output(DG_IntegerIndex(i3 = 8));
