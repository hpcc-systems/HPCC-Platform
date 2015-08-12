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

PSTRING := TYPE
EXPORT string load(string rawdata) := rawdata[2..TRANSFER(rawdata[1], unsigned integer1)];
EXPORT integer8 physicallength(string ecldata) := TRANSFER(LENGTH(ecldata), unsigned integer1) + 1;
EXPORT string store(string ecldata) := TRANSFER(LENGTH(ecldata), string1) + ecldata;
END;

r := { unsigned integer8 id, PSTRING firstname, PSTRING lastname };

ds := dataset([{1,'Gavin','Hawthorn'},{2,'Jim','Peck'}], r);
output(ds);

