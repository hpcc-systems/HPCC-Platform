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
