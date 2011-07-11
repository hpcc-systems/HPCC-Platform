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

