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



filenameRecord := record
string10        name;
string8         size;
            end;

myService := service
dataset(filenameRecord) doDirectory(string dir) : entrypoint='doDirectory';
end;


dataset(filenameRecord) doDirectory(string dir) := beginC++
    __lenResult = 36;
    __result = malloc(36);
    memcpy(__result, "Gavin.hql 00000001Nigel     00050000", 36);
ENDC++;


output(doDirectory('c:\\'));
output(myService.doDirectory('c:\\'));
