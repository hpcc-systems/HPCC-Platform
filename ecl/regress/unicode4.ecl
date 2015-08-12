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

inRecord :=
    RECORD,maxlength(10000)
        unicode100 firstname;
    END;

input := DATASET('~gnt', inRecord, CSV(MAXLENGTH(10000),terminator('\r\n')));

//output(input, {length(trim(firstname)), U'!' + trim(firstname[1..20]) + U'**$!'});
output(input, {length(trim(firstname)), (U'!' + firstname[1..5] + U'**$!'), 'abc'});
