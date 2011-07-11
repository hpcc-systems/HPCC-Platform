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

// global merge regression test (also tests large variable rows)
//noRoxie

import std.system.thorlib;
unsigned numrecs := 10000 : stored('numrecs');

fixedrec := record
         string90 payload;
         string5 key;
       end;

varrec := record, maxlength(2000000)
     string payload;
         string5 key;
       end;

dummy := dataset([{'A', '0'}], fixedrec);
thornodes := MAX(dummy, thorlib.nodes()) : global;  // Force it to calculate nodes() on thor not hthor


dotest(kind,rec) := macro

  #uniquename(seed)
  #uniquename(addNodeNum)
  #uniquename(one_per_node)
  #uniquename(generatePseudoRandom)
  #uniquename(sortedrecs1)
  #uniquename(sortedrecs2)
  #uniquename(sortedrecs3)
  #uniquename(sortedrecs4)
  #uniquename(sortedrecs)
  #uniquename(checksort)
  #uniquename(checksorted)


  %seed% := dataset([{'A', '0'}], rec);

  rec %addNodeNum%(rec L, unsigned4 c) := transform
        SELF.key := (string) c;
        SELF := L;
      END;

  %one_per_node% := distribute(normalize(%seed%, thorlib.nodes(), %addNodeNum%(LEFT, COUNTER)), (unsigned) key);

  rec %generatePseudoRandom%(rec L, unsigned4 c) := transform
      SELF.payload := (string) RANDOM() + (string) RANDOM()+(string) RANDOM() + (string) RANDOM()+(string) RANDOM() + (string) RANDOM();
      SELF.key := (string) (RANDOM() % 100000);
    END;

  %sortedrecs1% := sort(NORMALIZE(%one_per_node%, numrecs, %generatePseudoRandom%(LEFT, counter)), key);
  %sortedrecs2% := sort(NORMALIZE(%one_per_node%, numrecs, %generatePseudoRandom%(LEFT, counter)), key);
  %sortedrecs3% := sort(NORMALIZE(%one_per_node%, numrecs, %generatePseudoRandom%(LEFT, counter)), key);
  %sortedrecs4% := sort(NORMALIZE(%one_per_node%, numrecs, %generatePseudoRandom%(LEFT, counter)), key);
  %sortedrecs% := MERGE(%sortedrecs1%,%sortedrecs2%,%sortedrecs3%,%sortedrecs4%);

  rec %checksort%(rec l, rec r) := TRANSFORM
    self.key := if (l.key <= r.key, r.key, ERROR('ERROR - records not in sequence'));
    self := r;
  END;

  %checksorted% := iterate(%sortedrecs%, %checksort%(LEFT, RIGHT));

  if(count(%checksorted%) = (thornodes*numrecs*4), output(kind + ' sort succeeded'), FAIL(kind + ' ERROR- count did not match expected'));

endmacro;

dotest('Fixed',fixedrec);
dotest('Variable',varrec);
