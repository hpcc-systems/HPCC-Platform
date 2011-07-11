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


ds := dataset([{1, ['a','b','c']}], { unsigned id; dataset({string1 str}) x });

ds t(ds l) := TRANSFORM
    d := dedup(l.x, all)(str <> 'z');
    e := d(str <> (string)count(d));
    self.x := e;
    self := l;
END;

p := project(nofold(ds), t(LEFT));

output(p);
