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


rawFile := dataset([
            {'on'},
            {' on'},
            {' on '},
            {'On'},
            {' oN'},
            {'true'},
            {' true'},
            {' true '},
            {'TRue'},
            {' trUE'},
            {'1'},
            {'2'},
            {'9'},
            {'0001'},
            {'0009'},
            {'  100'},
            {' 0009 '},

            {'o n'},
            {'tru e'},
            {'0000'},
            {''}], { string line });

output(rawFile,,'~csvBool',csv,overwrite);

inFile := dataset('~csvBool', { boolean isOk }, csv);
output(inFile);
