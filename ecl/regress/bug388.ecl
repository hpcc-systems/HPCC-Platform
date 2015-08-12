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

MyRec := RECORD
string20 year_model;
string20 corp_maker_number;
string20 division_number;
string20 group_number;
string20 sub_group_number;
string20 id_number;
END;

cars_tbl := DATASET('car',MyRec,THOR);


Sorted_cars := SORT(cars_tbl,
cars_tbl.year_model,
cars_tbl.corp_maker_number,
cars_tbl.division_number,
cars_tbl.group_number,
cars_tbl.sub_group_number,
cars_tbl.id_number);

hh_deduped_vehicles := DEDUP(Sorted_cars, (LEFT.year_model =
RIGHT.year_model AND
LEFT.corp_maker_number = RIGHT.corp_maker_number AND
LEFT.division_number = RIGHT.division_number AND
LEFT.group_number = RIGHT.group_number AND
LEFT.sub_group_number = RIGHT.sub_group_number AND
LEFT.id_number = RIGHT.id_number));


output(hh_deduped_vehicles);
