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

//output(SAMPLE(TrainingKevinLogemann.File_People_Slim,100,1));
//output(Enth(TrainingKevinLogemann.File_People_Slim,100));
//output(choosen(TrainingKevinLogemann.File_People_Slim,100));
//output(TrainingKevinLogemann.File_People_Slim);

#option ('applyInstantEclTransformations', true);
#option ('applyInstantEclTransformationsLimit', 999);

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

File_People_Slim := dataset('x',namesRecord,FLAT);

output(SAMPLE(File_People_Slim,100,1));
output(choosen(File_People_Slim,100));
output(File_People_Slim);
