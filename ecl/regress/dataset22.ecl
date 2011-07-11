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

export t_StringArrayItem := record
            string value { xpath(''),MAXLENGTH (1024) };
end;

export t_Result := record
unsigned id;
dataset(t_StringArrayItem) List {xpath('List/Item'), MAXCOUNT(10)};
end;

ds := dataset([{1,['x','y']},{2,['gavin','rkc']}], t_Result);
output(ds);
