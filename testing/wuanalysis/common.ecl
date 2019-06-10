/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

Export common := Module
    export layout_visits := RECORD
        STRING20 User;
        STRING30 url;
        INTEGER8 timestamp;
    END;
    export testfile1 := 'regress::wuanalysis::largedata1';
    export testfile2 := 'regress::wuanalysis::largedata2';
End;

