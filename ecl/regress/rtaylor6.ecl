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

layout := RECORD
String3 field1;
varstring256 field2;
varstring256 field3;
varstring16 field4;
varstring16 field5;
END;

layout tra(layout realdata) := TRANSFORM
Self.field1 := 'ZZZ';
Self.field2 := 'ZZZ-12345678';
Self.field3 := 'ZZZZ12345789';
Self.field4 := 'ZZZZZZ';
Self.field5 := '';
END;

SeedRec := dataset([{'ZZZ','ZZZ-12345678','ZZZZ12345789','ZZZZZZ',''}],layout);
fake_data := normalize(seedrec,88377,tra(Left));
output(fake_data,,'RTTEMP::fake_data',OVERWRITE);

foo := DATASET('RTTEMP::fake_data',layout,THOR);
output(foo(field4='ZZZZZZ'));

