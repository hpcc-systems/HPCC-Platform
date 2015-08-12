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

#option ('optimizeIndexSource',true);

import sq;
sq.DeclareCommon();

unsigned zero := 0 : stored('zero');

output(count(sqSimplePersonBookIndex));
output(count(sqSimplePersonBookIndex)=10);
output(count(choosen(sqSimplePersonBookIndex, 20)));
output(count(choosen(sqSimplePersonBookIndex, 20))=10);
output(count(choosen(sqSimplePersonBookIndex, 4)));
output(count(choosen(sqSimplePersonBookIndex, 4))=4);
output(count(choosen(sqSimplePersonBookIndex, zero)));
output(count(choosen(sqSimplePersonBookIndex, zero))=0);

output(count(sqSimplePersonBookIndex(surname != 'Hawthorn')));
output(count(sqSimplePersonBookIndex(surname != 'Hawthorn'))=7);
output(count(choosen(sqSimplePersonBookIndex(surname != 'Hawthorn'), 20)));
output(count(choosen(sqSimplePersonBookIndex(surname != 'Hawthorn'), 20))=7);
output(count(choosen(sqSimplePersonBookIndex(surname != 'Hawthorn'), 3)));
output(count(choosen(sqSimplePersonBookIndex(surname != 'Hawthorn'), 3))=3);
output(count(choosen(sqSimplePersonBookIndex(surname != 'Hawthorn'), zero)));
output(count(choosen(sqSimplePersonBookIndex(surname != 'Hawthorn'), zero))=0);
