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

export MAC_Field_Declare(iscomp = false) := MACRO
string120  company_name := '' : stored('CompanyName');
string2 state_val := '' : stored('State');
string2 prev_state_val1l := '' : stored('OtherState1');
string2 prev_state_val2l := '' : stored('OtherState2');
string25 city_val := '' : stored('City');
  ENDMACRO;


export IFileSearch := interface
export string120  company_name;
export string2 state_val;
export string2 prev_state_val1l;
export string2 prev_state_val2l;
export string25 city_val;
        end;


export f(IFileSearch options) := options.company_name;

MAC_Field_Declare()

export CompatibleFileSearch := module(IFileSearch)
export string120  company_name := ^.company_name;
export string2    state_val    := ^.state_val;
export string2    prev_state_val1l := ^.prev_state_val1l;
export string2    prev_state_val2l := ^.prev_state_val2l;
export string25   city_val := ^.city_val;
    end;



output(f(CompatibleFileSearch));
