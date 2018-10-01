/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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

//version newIndexReadMapping=false
//version newIndexReadMapping=true,noroxie,nothor

// Testing various indexes that might present tricky cases for remote projection/filtering

import ^ as root;
newIndexReadMapping := #IFDEFINED(root.newIndexReadMapping, false);

#option ('newIndexReadMapping', newIndexReadMapping);

import Std.File AS FileServices;

import $.setup;
prefix := setup.Files(false, false).IndexPrefix + WORKUNIT + '::';

simple_rec := RECORD
  unsigned4 u4;
  integer1 s4;
  big_endian unsigned4 bu4;
  big_endian integer4 bs4
END;

simple_ds := DATASET([{1,1,1,1},{2,2,2,2},{3,-3,3,-3},{4,4,4,4}], simple_rec);

// Simple case - last field gets moved to fileposition and only 3 fields are considered keyed
simpleName := prefix + '1_simple.idx';
simple := INDEX(simple_ds, {simple_ds}, simpleName);

// All fields keyed, so no fileposition
allkeyedName := prefix + '2_allkeyed.idx';
allkeyed := INDEX(simple_ds, {simple_ds}, {}, allkeyedName);

// Only one field, so fileposition
onekeyedName := prefix + '3_onekeyed.idx';
onekeyed := INDEX(simple_ds, {s4}, onekeyedName);
 
// Check bias - keyed fields get bias (but only if signed), and so do unkeyed (except in filepos field)
payloadbiasName := prefix + '4_payloadbias.idx';
payloadbias := INDEX(simple_ds, {u4},{s4,bu4,bs4}, payloadbiasName);

payloadbias2Name := prefix + '5_payloadbias2.idx';
payloadbias2 := INDEX(simple_ds, {u4},{bu4,bs4,s4}, payloadbias2Name);

// Check for tricky types - nested records in payload
nestrecName := prefix + '6_nestrec.idx';
nestrec :=  INDEX(simple_ds, {u4}, {simple_rec r := ROW({u4,s4,bu4,bs4}, simple_rec) }, nestrecName);

// Check for unserializable types in payload
unserializedName := prefix + '7_ifrec.idx';
unserialized := INDEX(simple_ds, {u4}, { u4, ifblock(self.u4!=2) s4,END,bu4,bs4 }, unserializedName);

unserialized2Name := prefix + '8_ifrec.idx';
unserialized2 := INDEX(simple_ds, {u4}, { boolean b := u4!=2, ifblock(self.b) s4,bu4,bs4 END }, unserialized2Name);

// Check for nested ifblocks
unserialized3Name := prefix + '9_ifrec.idx';
unserialized3 := INDEX(simple_ds, {u4}, { u4, ifblock(self.u4!=2) s4, ifblock(self.u4 != 2) bu4 END,END,bs4 }, unserialized3Name);

// Check for tricky types - child datasets 
// Check for tricky types - utf8 
// Check for tricky types - unicode 
// Check for tricky types - blobs
// Check for tricky types - alien data types 
// Check for tricky types - sets in payload 

set_rec := RECORD
  unsigned4 u4;
  set of integer1 s4;
  set of big_endian unsigned4 bu4;
  set of big_endian integer4 bs4
END;

set_ds := DATASET([{1,[1,2,3],[1,2,3],[1,2,3]},{2,[2,3,4],[2,3,4],[2,3,4]},{3,[-3,-4,-5],[3,4,5],[-3,-4,-5]},{4,[4,5,6],[4,5,6],[4,5,6]}], set_rec);

keyedsetName := prefix + '10_setrec.idx';
keyedset := INDEX(set_ds, {u4},{s4,bu4,bs4 }, keyedsetName);

// Check for multipart keys, noroot keys, etc

SEQUENTIAL(
  OUTPUT('simple'),
  BUILDINDEX(simple, OVERWRITE),
  OUTPUT(choosen(simple, 5));
  //OUTPUT(choosen(simple(keyed(bu4=3),WILD(u4),WILD(s4)), 5)), 

  OUTPUT('allkeyed'),
  BUILDINDEX(allkeyed, OVERWRITE),
  OUTPUT(choosen(allkeyed, 5));
  //OUTPUT(choosen(allkeyed(keyed(bs4=-3),WILD(u4),WILD(s4),WILD(bu4)), 5)), 

  OUTPUT('onekeyed'),
  BUILDINDEX(onekeyed, OVERWRITE),
  OUTPUT(choosen(onekeyed, 5)); 
  //OUTPUT(choosen(onekeyed(keyed(s4=-3)), 5)),

  OUTPUT('payloadbias'),
  BUILDINDEX(payloadbias, OVERWRITE),
  OUTPUT(choosen(payloadbias, 5)); 
  //OUTPUT(choosen(payloadbias(keyed(u4=3)), 5)) 

  OUTPUT('payloadbias2'),
  BUILDINDEX(payloadbias2, OVERWRITE),
  OUTPUT(choosen(payloadbias2, 5)); 
  //OUTPUT(choosen(payloadbias2(keyed(u4=3)), 5)) 

  OUTPUT('nestrec'),
  BUILDINDEX(nestrec, OVERWRITE),
  OUTPUT(choosen(nestrec, 5)); 
  //OUTPUT(choosen(nestrec(keyed(u4=3)), 5)) 

  OUTPUT('unserialized'),
  BUILDINDEX(unserialized, OVERWRITE),
  OUTPUT(choosen(unserialized, 5)); 
  //OUTPUT(choosen(unserialized(keyed(u4=3)), 5)) 

  OUTPUT('unserialized2'),
  BUILDINDEX(unserialized2, OVERWRITE),
  OUTPUT(choosen(unserialized2, 5)); 
  //OUTPUT(choosen(unserialized2(keyed(u4=3)), 5)) 

  OUTPUT('unserialized3'),
  BUILDINDEX(unserialized3, OVERWRITE),
  OUTPUT(choosen(unserialized3, 5)); 
  //OUTPUT(choosen(unserialized3(keyed(u4=3)), 5)) 

  OUTPUT('keyedset'),
  BUILDINDEX(keyedset, OVERWRITE),
  OUTPUT(choosen(keyedset, 5)); 
  //OUTPUT(choosen(keyedset(keyed(u4=[3,4,5])), 5)) 


  // Clean-Up
  FileServices.DeleteLogicalFile(simpleName),
  FileServices.DeleteLogicalFile(allkeyedName),
  FileServices.DeleteLogicalFile(onekeyedName),
  FileServices.DeleteLogicalFile(payloadbiasName),
  FileServices.DeleteLogicalFile(payloadbias2Name),
  FileServices.DeleteLogicalFile(nestrecName),
  FileServices.DeleteLogicalFile(unserializedName),
  FileServices.DeleteLogicalFile(unserialized2Name),
  FileServices.DeleteLogicalFile(unserialized3Name),
  FileServices.DeleteLogicalFile(keyedsetName);
);
