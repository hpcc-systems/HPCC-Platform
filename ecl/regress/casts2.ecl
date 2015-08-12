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




// add columns in here if you want more arguments to test in your processing.
inRecord :=
            RECORD
integer         len;
string100       text;
ebcdic string10 etext;
            END;



inDataset := dataset('in.doo',inRecord, FLAT);

outRecord := RECORD
string50        text;
integer4        value;
            END;


outRecord doTransform(inRecord l) := TRANSFORM
            SELF.text := (varstring)l.text[1..l.len]+(varstring)'x';
            SELF.value := (integer)l.etext;
        END;

outDataset := project(inDataset, doTransform(LEFT));

output(outDataset,,'out.d00');
