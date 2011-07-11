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

//This is primarily here to ensure that roxie fails gracefully when a packet that is too large is sent from the master to the slave
//nothor
//nothorlcr
//nohthor

string s10 := (string10)'' : stored ('s10');
string s100 := s10 + s10 + s10 + s10 + s10 + s10 + s10 + s10 + s10 + s10 : stored ('s100');
string s1000 := s100 + s100 + s100 + s100 + s100 + s100 + s100 + s100 + s100 + s100 : stored ('s1000');
string s10000 := s1000 + s1000 + s1000 + s1000 + s1000 + s1000 + s1000 + s1000 + s1000 + s1000 : stored ('s10000');
string s100000 := s10000 + s10000 + s10000 + s10000 + s10000 + s10000 + s10000 + s10000 + s10000 + s10000 : stored ('s100000');
string myStoredString := 'x' + s100000 + 'x';


rec := { string x{maxlength(100)}, unsigned value };

ds := dataset([{'a',1},{'b',2},{'c',3},{'d',4}], rec);


f(dataset(rec) infile) := infile(x != myStoredString);

rec FailTransform := transform
  self.x := FAILMESSAGE[1..13]; 
  self.value := FAILCODE
END;

caught := catch(allnodes(f(ds)), onfail(FailTransform));

output(caught);

