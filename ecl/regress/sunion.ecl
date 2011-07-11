/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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




