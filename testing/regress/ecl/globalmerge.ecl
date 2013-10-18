/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

  %checksorted% := NOFOLD(iterate(%sortedrecs%, %checksort%(LEFT, RIGHT)));

  if(count(%checksorted%) = (thornodes*numrecs*4), output(kind + ' sort succeeded'), FAIL(kind + ' ERROR- count did not match expected'));

endmacro;

dotest('Fixed',fixedrec);
dotest('Variable',varrec);
