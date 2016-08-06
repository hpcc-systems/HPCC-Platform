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

postcodes := DATASET([{'KT19 1AA'}, {'KT19 1AB'}, {'KT19 1AC'}, {'KT19 1AD'},
                      {'KT19 1AE'}, {'KT19 1AF'}, {'KT19 1AG'}, {'KT19 1AH'}, 
                      {'KT19 1AI'}, {'KT19 1AJ'}, {'KT19 1AK'}, {'KT19 1AL'}, 
                      {'KT19 1AM'}, {'KT19 1AN'}, {'KT19 1AO'}, {'KT20 1AA'},
                      {'KT20 1AB'}, {'KT20 1AC'}, {'KT20 1AD'}, {'KT20 1AE'}, 
                      {'KT20 1AF'}, {'KT20 1AG'}, {'KT20 1AH'}, {'KT20 1AI'},
                      {'KT20 1AJ'}, {'KT20 1AK'}, {'KT20 1AL'}, {'KT20 1AM'}, 
                      {'KT20 1AN'}, {'KT20 1AO'}, {'KT30 1AA'}, {'KT30 1AB'}, 
                      {'KT30 1AC'}, {'KT30 1AD'}, {'KT30 1AE'}, {'KT30 1AF'}, 
                      {'KT30 1AG'}, {'KT30 1AH'}, {'KT30 1AI'}, {'KT30 1AJ'}, 
                      {'KT30 1AK'}, {'KT30 1AL'}, {'KT30 1AM'}, {'KT30 1AN'}, 
                      {'KT30 1AO'}, {'KT30 1AP'}], {string8 postcode});

outputraw := OUTPUT(postcodes,,'TST::postcodes', OVERWRITE);

Rawfile := DATASET('TST::postcodes', { string8 postcode, UNSIGNED8
                                      __filepos {virtual(fileposition)}}, FLAT);

INDX_Postcode := INDEX(Rawfile, {postcode, __filepos}, 'TST::postcode.key');
BuildIndexOp := BUILDINDEX(INDX_Postcode, OVERWRITE);

SET OF STRING4 PartialPostcode:= ['KT19','KT30'];
SET OF STRING4 PartialPostcodeStored:= ['KT19','KT30']:stored('PartialPostcode');
SET OF STRING PartialPostcodeStored2:= ['KT19','KT3']:stored('PartialPostcode2');
SET OF UNICODE PartialPostcodeStored3:= [U'KT19',U'KT3']:stored('PartialPostcode3');

partialmatch1 := INDX_Postcode( KEYED(postcode[1..4] IN PartialPostcode) );
partialmatch2 := INDX_Postcode( KEYED(postcode[1..4] IN PartialPostcodeStored) );
partialmatch3 := INDX_Postcode( KEYED(postcode[..4] IN PartialPostcodeStored) );
partialmatch4 := INDX_Postcode( KEYED(postcode[..4] IN PartialPostcodestored2) );

SEQUENTIAL(
  outputraw,
  BuildIndexOp,
  OUTPUT(partialmatch1),
  OUTPUT(partialmatch2),
  OUTPUT(partialmatch3),
  OUTPUT(partialmatch4),  
)
