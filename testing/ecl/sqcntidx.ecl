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

//UseStandardFiles
#option ('optimizeDiskSource',true)
#option ('optimizeChildSource',true)
#option ('optimizeIndexSource',true)


output(count(sqSimplePersonBookIndex));
output(count(sqSimplePersonBookIndex)=10);
output(count(choosen(sqSimplePersonBookIndex, 20)));
output(count(choosen(sqSimplePersonBookIndex, 20))=10);
output(count(choosen(sqSimplePersonBookIndex, 4)));
output(count(choosen(sqSimplePersonBookIndex, 4))=4);
output(count(choosen(sqSimplePersonBookIndex, 0)));
output(count(choosen(sqSimplePersonBookIndex, 0))=0);

output(count(sqSimplePersonBookIndex(surname != 'Halliday')));
output(count(sqSimplePersonBookIndex(surname != 'Halliday'))=7);
output(count(choosen(sqSimplePersonBookIndex(surname != 'Halliday'), 20)));
output(count(choosen(sqSimplePersonBookIndex(surname != 'Halliday'), 20))=7);
output(count(choosen(sqSimplePersonBookIndex(surname != 'Halliday'), 3)));
output(count(choosen(sqSimplePersonBookIndex(surname != 'Halliday'), 3))=3);
output(count(choosen(sqSimplePersonBookIndex(surname != 'Halliday'), 0)));
output(count(choosen(sqSimplePersonBookIndex(surname != 'Halliday'), 0))=0);
