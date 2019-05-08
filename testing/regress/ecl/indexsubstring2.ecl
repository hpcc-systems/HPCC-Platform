/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

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

import $.setup;
prefix := setup.Files(false, false).QueryFilePrefix;

#onwarning (4522, ignore);
#onwarning (4523, ignore);

//version multiPart=false,isKeyed=true
//version multiPart=false,isKeyed=false

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, false);
isKeyed := #IFDEFINED(root.isKeyed, true);

//--- end of version configuration ---

mkKeyed(boolean value) := IF(isKeyed, KEYED(value), value);

postcodes := DATASET([{D'KT19 1AA'}, {D'KT19 1AB'}, {D'KT19 1AC'}, {D'KT19 1AD'},
                      {D'KT20 1AE'}, {D'KT20 1AF'}, {D'KT20 1AG'}, {D'KT20 1AH'},
                      {D'KT21 1AI'}, {D'KT21 1AJ'}, {D'KT21 1AK'}, {D'KT21 1AL'},
                      {D'KT22 1AM'}, {D'KT22 1AN'}, {D'KT22 1AO'}, {D'KT22 1AA'},
                      {D'KT23 1AB'}, {D'KT23 1AC'}, {D'KT23 1AD'}, {D'KT23 1AE'},
                      {D'KT30 1AF'}, {D'KT30 1AG'}, {D'KT30 1AH'}, {D'KT30 1AI'},
                      {D'KT31 1AJ'}, {D'KT31 1AK'}, {D'KT31 1AL'}, {D'KT31 1AM'},
                      {D'KT32 1AN'}, {D'KT32 1AO'}, {D'KT32 1AA'}, {D'KT32 1AB'},
                      {D'KT40 1AC'}, {D'KT40 1AD'}, {D'KT40 1AE'}, {D'KT40 1AF'},
                      {D'KT41 1AG'}, {D'KT41 1AH'}, {D'KT41 1AI'}, {D'KT41 1AJ'},
                      {D'KT41 1AK'}, {D'KT41 1AL'}, {D'KT41 1AM'}, {D'KT41 1AN'},
                      {D'KT50 2AB'}, {D'KT50 3DE'}, {D'KT50 4FG'}, {D'KT50 5HI'},
                      {D'KT60 2AB'}, {D'KT60 3DE'}, {D'KT60 4FG'}, {D'KT60 5HI'},
                      {D'KT3'}, {D'KT4'},{D'KT50'}], {data8 postcode});

outputraw := OUTPUT(postcodes,,prefix + 'postcodes', OVERWRITE);


Rawfile := DATASET(prefix + 'postcodes', { DATA8 postcode, UNSIGNED8
                                      __filepos {virtual(fileposition)}}, FLAT);

INDX_Postcode := INDEX(Rawfile, {postcode, __filepos}, prefix + 'postcode.key');
BuildIndexOp := BUILDINDEX(INDX_Postcode, OVERWRITE);

SET OF DATA4 PartialPostcode:= [D'KT19',D'KT40',D'KT3\000',D'KT20 1AEEE',D'KT50',D'KT60 3DE'];
SET OF DATA4 PartialPostcodeStored:= [D'KT19',D'KT40',D'KT3\000',D'KT20 1AEEE', D'KT50',D'KT60 3DE']:stored('PartialPostcode');
SET OF DATA PartialPostcodeStored2:= [D'KT19',D'KT40',D'KT3\000',D'KT20 1AEEE', D'KT50',D'KT60 3DE']:stored('PartialPostcode2');
SET OF DATA8 PartialPostcodeStored8:= [D'KT19',D'KT40',D'KT3\000',D'KT50']:stored('PartialPostcode8');
SET OF DATA PartialPostcodeStoredX:= [D'KT19',D'KT40',D'KT3\000',D'KT50']:stored('PartialPostcodeX');
SET OF DATA3 PartialPostcodeStored2S:= [D'KT2']:stored('PartialPostcode2S');

partialmatch1 := INDX_Postcode( mkKeyed(postcode[1..4] IN PartialPostcode) );
partialmatch2 := INDX_Postcode( mkKeyed(postcode[1..4] IN PartialPostcodeStored) );
partialmatch3 := INDX_Postcode( mkKeyed(postcode[..4] IN PartialPostcodeStored) );
partialmatch4 := INDX_Postcode( mkKeyed(postcode[..4] IN PartialPostcodestored2) );
fullmatch1 := INDX_Postcode( mkKeyed(postcode IN PartialPostcodeStored) );
fullmatch2 := INDX_Postcode( mkKeyed(postcode IN PartialPostcodeStored2) );

four := 4 : stored('four');

partialmatch5 := INDX_Postcode( postcode[1..four] IN PartialPostcode );
partialmatch6 := INDX_Postcode( postcode[1..four] IN PartialPostcodeStored );
partialmatch7 := INDX_Postcode( postcode[..four] IN PartialPostcodeStored );
partialmatch8 := INDX_Postcode( postcode[..four] IN PartialPostcodestored2 );
partialmatch9 := INDX_Postcode( mkKeyed(postcode[..4] IN PartialPostcodestoredX) );
partialmatch10 := INDX_Postcode( mkKeyed(IF(four=4, postcode[..four] IN PartialPostcodestoredX,true)) );
partialmatch11 := INDX_Postcode( mkKeyed(postcode[..four] IN IF(four=4, PartialPostcodestoredX, all)) );
partialmatch12 := INDX_Postcode( mkKeyed(postcode[..four] IN PartialPostcodestoredX) OR mkKeyed(postcode[..3] IN PartialPostcodestored2S)   );
//None of these match because trailing \0 characters are not ignored.
partialmatch13 := INDX_Postcode( mkKeyed(postcode[..4] IN PartialPostcodestored8) );
partialmatch14 := INDX_Postcode( mkKeyed(IF(four=4, postcode[..four] IN PartialPostcodestored8,true)) );
partialmatch15 := INDX_Postcode( mkKeyed(postcode[..four] IN IF(four=4, PartialPostcodestored8, all)) );


z(dataset(recordof(INDX_Postcode)) ds) := TABLE(ds, { string8 postcode := (>string8<)ds.postcode, __filepos });

SEQUENTIAL(
  outputraw,
  BuildIndexOp,
  OUTPUT(z(partialmatch1)),  // match KT19*, KT40*, KT3, KT20*, KT50* and KT60*
  OUTPUT(z(partialmatch2)),  // match KT19*, KT40*, KT3, KT20*, KT50* and KT60*
  OUTPUT(z(partialmatch3)),  // match KT19*, KT40*, KT3, KT20*, KT50* and KT60*
  OUTPUT(z(partialmatch4)),  // match KT19*, KT40*, KT3 and KT50*
  OUTPUT(z(fullmatch1)),     // match KT3 and KT50
  OUTPUT(z(fullmatch2)),     // match KT3, KT50 and KT60 3DE
  OUTPUT(z(partialmatch5)),  // match KT19*, KT40*, KT3, KT20*, KT50* and KT60*
  OUTPUT(z(partialmatch6)),  // match KT19*, KT40*, KT3, KT20*, KT50* and KT60*
  OUTPUT(z(partialmatch7)),  // match KT19*, KT40*, KT3, KT20*, KT50* and KT60*
  OUTPUT(z(partialmatch8)),  // match KT19*, KT40*, KT3 and KT50*
  OUTPUT(z(partialmatch9)),  // match KT19*, KT40*, KT3 and KT50*
  OUTPUT(z(partialmatch10)),  // match KT19*, KT40*, KT3 and KT50*
  OUTPUT(z(partialmatch11)),  // match KT19*, KT40*, KT3 and KT50*
  OUTPUT(z(partialmatch12)),  // match KT19*, KT40*, KT3 and KT50*, and KT2*
  //OUTPUT(z(partialmatch13)),  // blank
  OUTPUT(z(partialmatch13)),  // blank
  OUTPUT(z(partialmatch14)),  // blank
  OUTPUT(z(partialmatch15)),  // blank
);
