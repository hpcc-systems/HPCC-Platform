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

#option ('targetClusterType', 'hthor');

EXPORT Layout_PairMatch := RECORD
  unsigned6 new_rid;
  unsigned6 old_rid;
  unsigned1  pflag;
END;

rec := record
    dataset(Layout_PairMatch) cd;
end;

ds := dataset([
{[{1600,405,0}]},
{[{1600,1350,0}]},
{[{405,1350,0}]},
{[{1350,433,0}]},
{[{433,1350,0}]}
],rec);


lilrec := recordof(ds);

bigrec := record, maxlength(10000)
    dataset(lilrec) cd;
end;

nada := dataset([{1}],{integer a});

bigrec makecd(nada l) := transform
    self.cd := ds;
end;

p2 := project(nada, makecd(left)) : global;
p := dataset('p', bigrec, thor);

output(p);
sum(p,  sizeof(p) );

//sizeof(ds)*count(ds);
