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

Layout_Match := record
unsigned6 rcid;
unsigned6 xyza;
string2   source;           // Source file type
qstring120 match_company_name;
qstring20 match_branch_unit;
qstring25 match_geo_city;
qstring10 prim_range;
qstring28 prim_name;
qstring8  sec_range;
unsigned3 zip;
end;

Company_Match_Init  := dataset('x', Layout_Match, thor);

Company_Match_Dedup := dedup(Company_Match_Init, match_company_name, match_branch_unit, match_geo_city, zip, prim_range, prim_name, xyza, all);

Company_Match_Dist := distribute(Company_Match_Dedup, hash(zip, trim(prim_name), trim(prim_range)));

output(Company_Match_Dist);

