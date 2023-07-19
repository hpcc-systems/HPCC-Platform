/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2023 HPCC Systems®.

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
//The library is defined and built in aaalibraryjava.ecl

IMPORT java;

STRING cat(SET OF STRING s) := IMPORT(java, 'javaembed_ex7.cat:([Ljava/lang/String;)Ljava/lang/String;');

namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

resultRecord := 
            RECORD
string30        fullname;
            END;

MyInterface(dataset(namesRecord) ds) := interface
    export dataset(resultRecord) result;
    export integer initialized;
end;

appendDataset(dataset(namesRecord) ds) := library('aaaLibraryJava',MyInterface(ds));

namesTable := dataset([
        {'Halliday','Gavin',31},
        {'Halliday','Liz',30},
        {'Jones','John', 44},
        {'Smith','George',75},
        {'Smith','Baby', 2}], namesRecord);

integer init := appendDataset(namesTable).initialized : ONCE;

output(init);
appended := appendDataset(namesTable);
output(appended.result);
// Call java from both query and library
output(cat(['Hello','World']));
