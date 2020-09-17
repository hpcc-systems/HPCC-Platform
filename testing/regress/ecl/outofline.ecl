/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

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

Layout_Test := RECORD
  DECIMAL32_10 DecimalField;
  REAL8 RealField;
END;

FieldValFunc(Layout_Test le,UNSIGNED2 FN) := DEFINE CASE(FN,1 => le.DecimalField,2 => le.RealField,0);

FieldValFunc(DATASET([{1.234, 2.5}], Layout_Test)[1], 1);


createDecimal(real x) := DEFINE (decimal32_31)x;

pi := 3.14159265;
output(NOFOLD(pi));

one := 1 : stored('one');
oneD := createDecimal(one);
output(oneD + oneD);

//Check that it preserves 27 significant digits...
output(createDecimal(pi) + createDecimal(pi/1.0e9)  + createDecimal(pi/1.0e18));
