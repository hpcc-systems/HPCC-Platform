/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

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

DataRec := RECORD
    UNSIGNED1       x;
    UNSIGNED1       y;
    UNSIGNED1       z := 0;
END;

ds := DATASET
    (
        [
            {12, 14},
            {22, 24},
            {32, 34}
        ],
        DataRec
    );

MaybeAdd(inFile, destField, field1 = '', field2 = '') := FUNCTIONMACRO
    LOCAL result := PROJECT
        (
            inFile,
            TRANSFORM
                (
                    RECORDOF(inFile),
                    SELF.destField := #IF(#TEXT(field1) != '')
                                          LEFT.field1
                                      #ELSE
                                          0
                                      #END
                                      +
                                      #IF(#TEXT(field2) != '')
                                          LEFT.field2
                                      #ELSE
                                          0
                                      #END ,
                    SELF := LEFT
                )
        );
    RETURN result;
ENDMACRO;

res := MaybeAdd
    (ds,z,/**/field2 := y);

res2 := MaybeAdd
    (ds,z,
    //
    field2 := y);

res3 := MaybeAdd
    (ds,z,/**/NAMED field2 := y);

res4 := MaybeAdd
    (ds,z,/**/field2 /**/ := /**/ y /**/);

res;
res2;
res3;
res4;
