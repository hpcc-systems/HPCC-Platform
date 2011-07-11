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

