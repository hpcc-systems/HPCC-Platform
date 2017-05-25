-----BEGIN PGP SIGNED MESSAGE-----
Hash: SHA1

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
                      {'KT3'}, {'KT4'}], {string8 postcode});


outputraw := OUTPUT(postcodes,,'TST::postcodes', OVERWRITE);

outputraw;
-----BEGIN PGP SIGNATURE-----
Version: GnuPG v1

iQEcBAEBAgAGBQJZJuqQAAoJEG37B3CNqkF9rYkIALn8qRE0jYvcBd+seNhT+cRL
MRQGbPGdv4XTRnToJVvmMztglTp9o0jmpnbCiAQ7iSrJLBr2YRHesY5BtFM2RcOD
uQ2UxxHOOAUk3KcXY/QSnWR8vWj2dCwPzqkIFvf9NWWZiMjwpKeFuklYa7u8xS8R
UlmLFvFdtaa9SJjzMVjKh6WgQaCjiK7GV4vqDytNXIPczX/in1NoIX0FbxZicKzj
Jt4jhE1oymvw+KlgmvrVs/Mi1V0wIwGGQE4kNzBxK79zww9ttN1GlBD6knrzSDgv
2FSo1618z20DdZY+sGlTPZ2XRVs+xL9EzIe2FrvmtjjGvKVngrGsNzulDqhVnoE=
=ovMK
-----END PGP SIGNATURE-----
