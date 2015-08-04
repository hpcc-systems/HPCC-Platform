/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

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

string descrText := 'only the listed fields should show in form, xsd, and wsdl<br/>and in the given order<br/><br/>';
string helpText := 'Enter some values and hit submit';

#webservice(fields('u1', 'i1'), help(helpText), description(descrText));
#webservice(help('can not use #webservice more than once'), description('multiple #webservice error'));

integer1 i1 := 0 : stored('i1');
unsigned1 u1 := 0 : stored('u1');

output (i1, named('i1'));
output (u1, named('u1'));

