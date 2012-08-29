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

