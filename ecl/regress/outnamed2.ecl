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

r1 :=   record
unsigned8       id;
string          name;
        end;

r2 :=   record
unsigned8       id{named('ID')};
string          name{named('Name')};
        end;



output(100);
output(100, named('Id'));
output(dataset([{1,'Gavin'}], r1));
output(dataset([{1,'Gavin'}], r2));
