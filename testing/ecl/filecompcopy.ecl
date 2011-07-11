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

//noroxie 
//skip nodfu       

// testing compressed->compressed, expanded->compressed, and compressed->expanded copies
// requires dfuserver
// NOTE: add nodfu=1 in regression.ini or local.ini to avoid test


import Std.File AS FileServices;


unsigned8 numrecs := 100000/CLUSTERSIZE : stored('numrecs');   // rows per node

rec := record
     string10  key;
     string10  seq;
     string80  fill;
       end;

seed := dataset([{'0', '0', '0'}], rec);

rec addNodeNum(rec L, unsigned4 c) := transform
    SELF.seq := (string) (c-1);
    SELF := L;
  END;

one_per_node := distribute(normalize(seed, CLUSTERSIZE, addNodeNum(LEFT, COUNTER)), (unsigned) seq);

rec fillRow(rec L, unsigned4 c) := transform

    SELF.key := (>string1<)(RANDOM()%95+32)+
                (>string1<)(RANDOM()%95+32)+
                (>string1<)(RANDOM()%95+32)+
                (>string1<)(RANDOM()%95+32)+
                (>string1<)(RANDOM()%95+32)+
                (>string1<)(RANDOM()%95+32)+
                (>string1<)(RANDOM()%95+32)+
                (>string1<)(RANDOM()%95+32)+
                (>string1<)(RANDOM()%95+32)+
                (>string1<)(RANDOM()%95+32);

    unsigned4 n := ((unsigned4)L.seq)*numrecs+c;
    SELF.seq := (string10)n;
    SELF.fill := (string80)RANDOM();
  END;

outdata := NORMALIZE(one_per_node, numrecs, fillRow(LEFT, counter));  

copiedcmp1 := DATASET('nhtest::testfile_exp_copy_cmp', rec, flat, __compressed__);
copiedcmp2 := DATASET('nhtest::testfile_cmp_copy_cmp', rec, flat, __compressed__);
copiedexp := DATASET('nhtest::testfile_cmp_copy_exp', rec, flat);

unsigned compareDatasets(dataset(rec) ds1,dataset(rec) ds2) := FUNCTION
   RETURN COUNT(JOIN(SORTED(ds1,(unsigned4)seq),SORTED(ds2,(unsigned4)seq),(unsigned4)left.seq=(unsigned4)right.seq,FULL ONLY,LOCAL));
END;


sequential (
  OUTPUT(outdata,,'nhtest::testfile_exp',overwrite),
  OUTPUT(outdata,,'nhtest::testfile_cmp',overwrite,__compressed__),
  
// test copy expanded to compressed  

  FileServices.Copy('nhtest::testfile_exp',                 // sourceLogicalName
            '',                         // destinationGroup
            'nhtest::testfile_exp_copy_cmp',            // destinationLogicalName
            ,                           // sourceDali
            5*60*1000,                      // timeOut 
            ,                               // espServerIpPort
            ,                               // maxConnections, 
            true,                           // allowoverwrite
            true,                       // replicate
            ,                           // asSuperfile, 
            true                        // compress
           ),

// test copy compressed to compressed  

  FileServices.Copy('nhtest::testfile_cmp',                 // sourceLogicalName
            '',                         // destinationGroup
            'nhtest::testfile_cmp_copy_cmp',            // destinationLogicalName
            ,                           // sourceDali
            5*60*1000,                      // timeOut 
            ,                               // espServerIpPort
            ,                               // maxConnections, 
            true,                           // allowoverwrite
            true,                       // replicate
            ,                           // asSuperfile, 
            true                        // compress
           ),  


// test copy compressed to expanded  

  FileServices.Copy('nhtest::testfile_cmp',                 // sourceLogicalName
            '',                         // destinationGroup
            'nhtest::testfile_cmp_copy_exp',            // destinationLogicalName
            ,                           // sourceDali
            5*60*1000,                      // timeOut 
            ,                               // espServerIpPort
            ,                               // maxConnections, 
            true,                           // allowoverwrite
            true,                       // replicate=false, 
            ,                           // asSuperfile, 
            false                       // compress
           ),
           
   OUTPUT(compareDatasets(outdata,copiedcmp1)),
   OUTPUT(compareDatasets(outdata,copiedcmp2)),
   OUTPUT(compareDatasets(outdata,copiedexp))
           
 );

