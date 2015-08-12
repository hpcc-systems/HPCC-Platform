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

#option ('childQueries', true);
//Working

import sq;
sq.DeclareCommon();

unsigned zero := 0 : stored('zero');

output(count(sqHouseDs));
output(count(sqHouseDs)=5);
output(count(choosen(sqHouseDs, 10)));
output(count(choosen(sqHouseDs, 10))=5);
output(count(choosen(sqHouseDs, 4)));
output(count(choosen(sqHouseDs, 4))=4);
output(count(choosen(sqHouseDs, zero)));
output(count(choosen(sqHouseDs, zero))=0);

output(count(sqHouseDs(postCode != 'WC1')));
output(count(sqHouseDs(postCode != 'WC1'))=4);
output(count(choosen(sqHouseDs(postCode != 'WC1'), 10)));
output(count(choosen(sqHouseDs(postCode != 'WC1'), 10))=4);
output(count(choosen(sqHouseDs(postCode != 'WC1'), 3)));
output(count(choosen(sqHouseDs(postCode != 'WC1'), 3))=3);
output(count(choosen(sqHouseDs(postCode != 'WC1'), zero)));
output(count(choosen(sqHouseDs(postCode != 'WC1'), zero))=0);
