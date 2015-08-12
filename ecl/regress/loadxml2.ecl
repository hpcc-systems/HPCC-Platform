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

LOADXML('<ABCSERVICE><ABC>129970464</ABC><DID></DID><name>Gavin</name></ABCSERVICE>');

string9 abc_value := %'abc'% : stored('abc');
string14 did_value := %'did'% : stored('did');

output('Abc = ' + abc_value);
output('did = ' + did_value);
