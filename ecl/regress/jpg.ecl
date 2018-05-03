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

JPEG(INTEGER len) := TYPE

            EXPORT DATA LOAD(DATA D) := D[1..len];

            EXPORT DATA STORE(DATA D) := D[1..len];

            EXPORT INTEGER PHYSICALLENGTH(DATA D) := len;

END;



export Layout_imgdb := RECORD, MAXLENGTH(50000)

            STRING1 ID_L;

            UNSIGNED5 ID_N;

            UNSIGNED2 DATE;

            UNSIGNED2 LEN;

            JPEG(SELF.LEN) PHOTO;

            UNSIGNED8 _FPOS { VIRTUAL(FILEPOSITION) };

END;




d := dataset('victor::imga', Layout_imgdb, flat, virtual(legacy));

// e := distribute(d, RANDOM());
// output(e,,'victor::imga.dist', overwrite);

output(choosen(d, 50));
