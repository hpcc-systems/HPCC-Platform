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

person := dataset('person', { unsigned8 person_id, string1 per_sex, string10 per_first_name, string10 per_last_name }, thor);

personx := choosen(person,20);

rec1 := RECORD
    string15 fieldn := 'GENDER';
    string5 valuen := (string5)person.per_sex;
    integer cnt := COUNT(GROUP);
END;

tab1 := TABLE(personx, rec1, rec1.fieldn, rec1.valuen);

OUTPUT(tab1);

// -----------------------------------------
rec2 := RECORD
    string15 fieldn := 'LAST_NAME';
    string5 valuen := (string5)person.per_last_name;
    integer cnt := COUNT(GROUP);
END;

tab2 := TABLE(personx, rec2, rec2.fieldn, rec2.valuen);

OUTPUT(tab2);

tab := tab1 + tab2;

OUTPUT(tab);

// -----------------------------------------------------------
/*
tab1 := CREATE_TABLE(dms_person, 'GENDER', person.per_gender_code);

tab2 := CREATE_TABLE(dms_person, 'MARITAL_STATUS', dms_person.per_marital_status_code);

tab := tab1 + tab2;

OUTPUT(tab);
*/
// -----------------------------------------------------------




