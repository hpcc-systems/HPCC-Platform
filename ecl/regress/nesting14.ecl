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



nameRecord := RECORD
        string20    surname;
        string10    forename;
        string1     initial := '';
    END;


personRecord := RECORD
    nameRecord      primary;
    nameRecord      mother;
    nameRecord      father;
                END;


personDataset := DATASET([
        {{'James','Walters','C'},{'Jessie','Blenger'},{'Horatio','Walters'}},
        {{'Anne','Winston'},{'Sant','Aclause'},{'Elfin','And'}}], personRecord);

output(personDataset);


personDataset2 := DATASET([
        {'James','Walters','C','Jessie','Blenger','','Horatio','Walters',''},
        {'Anne','Winston','','Sant','Aclause','','Elfin','And',''}], personRecord);

output(personDataset2);

