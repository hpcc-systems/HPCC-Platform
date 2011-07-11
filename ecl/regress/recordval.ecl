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

inf := dataset('xxx::yyy', {unsigned8 did, string9 ssn}, THOR);

Layout_PairMatch := record
  unsigned1  pflag;
  end;

sm_rec := record
    inf.ssn;
  end;

me_use := table(inf,{ inf.ssn; });   // works

Layout_PairMatch tra(me_use ll, me_use r) := transform
  self.pflag := 1;
  end;

mu1 := me_use(ssn<>'');

j := join(mu1,mu1, left.ssn=right.ssn, tra(left,right));

output(j,,'TEMP:idRules_0');