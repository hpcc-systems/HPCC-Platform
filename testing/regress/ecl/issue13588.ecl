/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems.

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


xRec := RECORD
    integer x;
END;

idRecord := RECORD
    integer         id;
    string          name;
    DATASET(xRec)   x;
END;

rowRecord := RECORD
    integer id;
    idRecord r;
END;

ds := DATASET([
        {1,{2,'gavin',[{1},{2}]}},
        {2,{3,'john',[{-1},{-5}]}}], rowRecord);

i := INDEX(ds, { id }, { ds }, 'REGRESS::TEMP::ISSUE13588');
BUILD(i,OVERWRITE);
OUTPUT(i,OVERWRITE);
