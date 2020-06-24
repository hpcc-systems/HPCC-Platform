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


numberRecord :=
            RECORD
string2000000   number;
            END;

number2Record :=
            RECORD
string2000001   number;
            END;

numbersRecord := RECORD
  EMBEDDED DATASET(number2Record) numbers;
END;



ds := DATASET(3000, transform(numberRecord, SELF.number := (string)COUNTER));

r := ROLLUP(NOFOLD(GROUP(ds, number[50000], LOCAL)), GROUP,
            transform(numbersRecord, SELF.numbers := project(ROWS(LEFT),
                                                             transform(number2Record, SELF.number := LEFT.number))));

output(count(nofold(r)));
