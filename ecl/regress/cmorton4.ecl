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

export Layout_Desc :=
RECORD
    STRING4 hri;
    STRING desc;
END;

r := record
string9 xyz;
string1 valid_xyz;
dataset(Layout_Desc) hri_xyz;
end;

d := dataset([
{'347031328', []},
{'076482587', []},
{'347031328', []},
{'097590172', []},
{'602935256', []},
{'923269240', []},
{'338746452', []},
{'801877227', []},
{'923269240', []},
{'427501145', []},
{'393723262', []}], r);

output(d)
