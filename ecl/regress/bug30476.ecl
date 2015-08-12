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

layout_city := record
unsigned4 UniqueID;
string25    city;
end;

my_cities := dataset([{1,'Boynton Beach'},{1,'Cincinnati, OH'}], layout_city);

layout_names := record
unsigned4 UniqueID;
string20    fname;
string20    mname;
set of string cities;
end;

names := dataset([{1,'Tony','Middleton',[]}], layout_names);

// Denormalize cities
layout_names AppendCities(names l, my_cities r) := transform
    self.cities := l.cities + [r.city];
    self := l;
end;

names_with_cities := denormalize(names,
                                 my_cities,
                                 left.UniqueID = right.UniqueID,
                                 AppendCities(left, right));

output(names_with_cities);
