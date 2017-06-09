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

#option ('warnOnImplicitReadLimit', true);

//version multiPart=false

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, false);

//--- end of version configuration ---

postcodes := DATASET([{'KT19 1AA'}, {'KT19 1AB'}, {'KT19 1AC'}, {'KT19 1AD'},
                      {'KT20 1AE'}, {'KT20 1AF'}, {'KT20 1AG'}, {'KT20 1AH'},
                      {'KT21 1AI'}, {'KT21 1AJ'}, {'KT21 1AK'}, {'KT21 1AL'},
                      {'KT22 1AM'}, {'KT22 1AN'}, {'KT22 1AO'}, {'KT22 1AA'},
                      {'KT23 1AB'}, {'KT23 1AC'}, {'KT23 1AD'}, {'KT23 1AE'},
                      {'KT30 1AF'}, {'KT30 1AG'}, {'KT30 1AH'}, {'KT30 1AI'},
                      {'KT31 1AJ'}, {'KT31 1AK'}, {'KT31 1AL'}, {'KT31 1AM'},
                      {'KT32 1AN'}, {'KT32 1AO'}, {'KT32 1AA'}, {'KT32 1AB'},
                      {'KT40 1AC'}, {'KT40 1AD'}, {'KT40 1AE'}, {'KT40 1AF'},
                      {'KT41 1AG'}, {'KT41 1AH'}, {'KT41 1AI'}, {'KT41 1AJ'},
                      {'KT41 1AK'}, {'KT41 1AL'}, {'KT41 1AM'}, {'KT41 1AN'},
                      {'KT50 2AB'}, {'KT50 3DE'}, {'KT50 4FG'}, {'KT50 5HI'},
                      {'KT60 2AB'}, {'KT60 3DE'}, {'KT60 4FG'}, {'KT60 5HI'},
                      {'KT3'}, {'KT4'},{'KT50'}], {string8 postcode});

outputraw := OUTPUT(postcodes,,'TST::postcodes', OVERWRITE);


Rawfile := DATASET('TST::postcodes', { string8 postcode, UNSIGNED8
                                      __filepos {virtual(fileposition)}}, FLAT);

INDX_Postcode := INDEX(Rawfile, {postcode, __filepos}, 'TST::postcode.key');
BuildIndexOp := BUILDINDEX(INDX_Postcode, OVERWRITE);

SET OF STRING4 PartialPostcode:= ['KT19','KT40','KT3 ','KT20 1AEEE','KT50','KT60 3DE'];
SET OF STRING4 PartialPostcodeStored:= ['KT19','KT40','KT3 ','KT20 1AEEE', 'KT50','KT60 3DE']:stored('PartialPostcode');
SET OF STRING PartialPostcodeStored2:= ['KT19','KT40','KT3 ','KT20 1AEEE', 'KT50','KT60 3DE']:stored('PartialPostcode2');

partialmatch1 := INDX_Postcode( KEYED(postcode[1..4] IN PartialPostcode) );
partialmatch2 := INDX_Postcode( KEYED(postcode[1..4] IN PartialPostcodeStored) );
partialmatch3 := INDX_Postcode( KEYED(postcode[..4] IN PartialPostcodeStored) );
partialmatch4 := INDX_Postcode( KEYED(postcode[..4] IN PartialPostcodestored2) );
fullmatch1 := INDX_Postcode( KEYED(postcode IN PartialPostcodeStored) );
fullmatch2 := INDX_Postcode( KEYED(postcode IN PartialPostcodeStored2) );

four := 4 : stored('four');

partialmatch5 := INDX_Postcode( postcode[1..four] IN PartialPostcode );
partialmatch6 := INDX_Postcode( postcode[1..four] IN PartialPostcodeStored );
partialmatch7 := INDX_Postcode( postcode[..four] IN PartialPostcodeStored );
partialmatch8 := INDX_Postcode( postcode[..four] IN PartialPostcodestored2 );

SEQUENTIAL(
  outputraw,
  BuildIndexOp,
  OUTPUT(partialmatch1),  // match KT19*, KT40*, KT3, KT20*, KT50* and KT60*
  OUTPUT(partialmatch2),  // match KT19*, KT40*, KT3, KT20*, KT50* and KT60*
  OUTPUT(partialmatch3),  // match KT19*, KT40*, KT3, KT20*, KT50* and KT60*
  OUTPUT(partialmatch4),  // match KT19*, KT40*, KT3 and KT50*
  OUTPUT(fullmatch1),     // match KT3 and KT50
  OUTPUT(fullmatch2),     // match KT3, KT50 and KT60 3DE
  OUTPUT(partialmatch5),  // match KT19*, KT40*, KT3, KT20*, KT50* and KT60*
  OUTPUT(partialmatch6),  // match KT19*, KT40*, KT3, KT20*, KT50* and KT60*
  OUTPUT(partialmatch7),  // match KT19*, KT40*, KT3, KT20*, KT50* and KT60*
  OUTPUT(partialmatch8),  // match KT19*, KT40*, KT3 and KT50*
);
