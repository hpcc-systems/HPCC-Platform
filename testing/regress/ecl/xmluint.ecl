/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

OutRecord :=
    RECORD
        UNSIGNED8 id;
        string val{xpath('id')};
    END;
 
rec := FROMXML(OutRecord,'<Row><id>18196239629641154791</id></Row>');
OUTPUT(rec, NAMED('FromXML'), NOXPATH);
 
OUTPUT(PROJECT(DATASET(rec), TRANSFORM(OutRecord,SELF.id := (UNSIGNED8)LEFT.val; SELF := LEFT)), NAMED('proj'), NOXPATH);
