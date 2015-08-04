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

export MAC_Field_Declare(iscomp = false) := MACRO
string120  company_name := '' : stored('CompanyName');
string2 state_val := '' : stored('State');
string2 prev_state_val1l := '' : stored('OtherState1');
string2 prev_state_val2l := '' : stored('OtherState2');
string25 city_val := '' : stored('City');
  ENDMACRO;


export ICompanySearch := interface
export string120  company_name;
        end;

ssnExceptRecord := record
unsigned6               ssn;
boolean                 always;
                    end;

export IFileSearch := interface(ICompanySearch)
export string2 state_val;
export string2 prev_state_val1l := 'fl';
export string2 prev_state_val2l;
export string25 city_val;
export set of string2 searchState;
export dataset(ssnExceptRecord) exceptions;
        end;


export f(IFileSearch options) := options.company_name;

MAC_Field_Declare()

export CompatibleFileSearch := stored(IFileSearch);

output(CompatibleFileSearch, ICompanySearch);
