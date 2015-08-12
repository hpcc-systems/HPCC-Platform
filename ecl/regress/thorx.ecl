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

citylayout := record
  ebcdic string2 state;
  ebcdic string4 abbr_city;
  string3 zip; // or is it 3
  integer1 metro_code;
  ebcdic string20 city;
end;

cityzips := dataset('cityzips2.hex', citylayout, flat);

cityascii := record
  string2 state;
  string4 abbr_city;
  string3 zip;
  integer1 metro_code;
  string20 city;
end;

cityascii fixem(citylayout input) :=
    transform
      self.state := input.state;
      self.abbr_city := input.abbr_city;
      self.zip := input.zip;
      self.metro_code := input.metro_code;
      self.city := input.city;
    end;

output(project(cityzips, fixem(left)));

