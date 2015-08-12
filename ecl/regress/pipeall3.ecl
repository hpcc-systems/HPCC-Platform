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

namesRecord :=
        RECORD
string10            forename;
string10            surname;
string2             nl := '\r\n';
        END;

output(PIPE('pipeRead csv1', namesRecord, CSV),,'csv1');
output(PIPE('pipeRead csv2', namesRecord, CSV(QUOTE([]))),,'csv2');

output(PIPE('pipeRead xml1', namesRecord, XML),,'xml1');
output(PIPE('pipeRead xml2', namesRecord, XML('abc/def/ghi')),,'xml2');
