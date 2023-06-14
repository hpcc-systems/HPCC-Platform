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
//publish

#option ('targetService', 'aaaLibraryJava');
#option ('createServiceAlias', true);

IMPORT java;

STRING cat(SET OF STRING s) := IMPORT(java, 'javaembed_library.cat:([Ljava/lang/String;)Ljava/lang/String;' : time);
INTEGER checkInitialized() := IMPORT(java, 'javaembed_library.queryInit:()I' : time);

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

appendDatasetLibrary(dataset(namesRecord) ds) := module,library(MyInterface)
  resultRecord t(namesRecord r) := TRANSFORM
    SELF.fullname := cat([r.forename, r.surname]);
  END;

  export result := PROJECT(ds, t(LEFT));
  export integer initialized := checkInitialized();
end;

build(appendDatasetLibrary);
