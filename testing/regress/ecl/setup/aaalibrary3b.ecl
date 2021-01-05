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

//nohthor
//nothor
//publish

#option ('targetService', 'aaaLibrary3b');
#option ('createServiceAlias', true);

//Example of nested 
namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

FilterLibrary(dataset(namesRecord) ds, string search, boolean onlyOldies) := interface
    export dataset(namesRecord) included;
    export dataset(namesRecord) excluded;
    export unsigned someValue;      // different interface from 1.2 in library2
end;


HallidayLibrary(dataset(namesRecord) ds) := interface
    export dataset(namesRecord) included;
    export dataset(namesRecord) excluded;
end;


HallidayLibrary3(dataset(namesRecord) ds) := module,library(HallidayLibrary)
    shared baseLibrary := LIBRARY('aaaLibrary3a', filterLibrary(ds, 'Halliday', false));
    export included := baseLibrary.included;
    export excluded := baseLibrary.excluded;
end;

BUILD(HallidayLibrary3);
