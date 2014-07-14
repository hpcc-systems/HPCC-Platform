/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

//skip type==thorlcr TBD
//nothor

MyFixedRec := RECORD
    STRING1 Value1;
    STRING1 Value2;
END;

FixedFile := DATASET([{'C','G'},
             {'C','C'},
             {'A','X'},
             {'B','G'},
             {'A','B'}],MyFixedRec);

MyVarRec := RECORD
    STRING1 Value1;
    STRING Value2;
END;

VarFile := DATASET([{'C','G'},
             {'C','C'},
             {'A','X'},
             {'B','G'},
             {'A','B'}],MyVarRec);

dedup1 := DEDUP(VarFile, Value2, ALL, HASH);
dedup2 := DEDUP(FixedFile, Value2, ALL, HASH);

sequential(
  output(dedup1),
  output(dedup2)
);
