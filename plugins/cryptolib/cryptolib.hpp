/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

#ifndef CRYPTOLIB_INCL
#define CRYPTOLIB_INCL

#ifdef _WIN32
#define CRYPTOLIB_CALL _cdecl
#else
#define CRYPTOLIB_CALL
#endif

#ifdef CRYPTOLIB_EXPORTS
#define CRYPTOLIB_API DECL_EXPORT
#else
#define CRYPTOLIB_API DECL_IMPORT
#endif

#include "hqlplugins.hpp"
#include "eclhelper.hpp"

extern "C"
{

#ifdef CRYPTOLIB_EXPORTS
  CRYPTOLIB_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb);
  CRYPTOLIB_API void setPluginContext(IPluginContext * _ctx);
#endif


//Following prototypes can be found in workunit CPP file (/var/lib/HPCCSystems/myeclccserver)

//Hashing encryption/decryption
//CRYPTOLIB_API void CRYPTOLIB_CALL clInstalledHashAlgorithms(bool & __isAllResult, size32_t & __lenResult, void * & __result);
CRYPTOLIB_API void CRYPTOLIB_CALL clSupportedHashAlgorithms(bool & __isAllResult, size32_t & __lenResult, void * & __result);
CRYPTOLIB_API void CRYPTOLIB_CALL clHash(size32_t & __lenResult,void * & __result,const char * algorithm,size32_t lenInputdata,const void * inputdata);


//Cipher symmetric encryption/decryption
//CRYPTOLIB_API void CRYPTOLIB_CALL clInstalledSymmetricCipherAlgorithms(bool & __isAllResult, size32_t & __lenResult, void * & __result);
CRYPTOLIB_API void CRYPTOLIB_CALL clSupportedSymmetricCipherAlgorithms(bool & __isAllResult, size32_t & __lenResult, void * & __result);

CRYPTOLIB_API void CRYPTOLIB_CALL clSymmetricEncrypt(size32_t & __lenResult,void * & __result,const char * algorithm,const char * key,size32_t lenInputdata,const void * inputdata);
CRYPTOLIB_API void CRYPTOLIB_CALL clSymmetricDecrypt(size32_t & __lenResult,void * & __result,const char * algorithm,const char * key,size32_t lenEncrypteddata,const void * encrypteddata);

CRYPTOLIB_API void CRYPTOLIB_CALL clSymmEncrypt(size32_t & __lenResult,void * & __result,const char * algorithm,size32_t lenKey,const void * key,size32_t lenInputdata,const void * inputdata);
CRYPTOLIB_API void CRYPTOLIB_CALL clSymmDecrypt(size32_t & __lenResult,void * & __result,const char * algorithm,size32_t lenKey,const void * key,size32_t lenEncrypteddata,const void * encrypteddata);

//Public Key encryption/decryption

//CRYPTOLIB_API void CRYPTOLIB_CALL clInstalledPublicKeyAlgorithms(bool & __isAllResult, size32_t & __lenResult, void * & __result);
//from file system
CRYPTOLIB_API void CRYPTOLIB_CALL clSupportedPublicKeyAlgorithms(bool & __isAllResult, size32_t & __lenResult, void * & __result);

CRYPTOLIB_API void CRYPTOLIB_CALL clPKIEncrypt(size32_t & __lenResult,void * & __result,const char * pkalgorithm,const char * publickeyfile,const char * passphrase,size32_t lenInputdata,const void * inputdata);
CRYPTOLIB_API void CRYPTOLIB_CALL clPKIDecrypt(size32_t & __lenResult,void * & __result,const char * pkalgorithm,const char * privatekeyfile,const char * passphrase,size32_t lenEncrypteddata,const void * encrypteddata);

CRYPTOLIB_API void CRYPTOLIB_CALL clPKISign(size32_t & __lenResult,void * & __result,const char * pkalgorithm,const char * privatekeyfile,const char * passphrase,size32_t lenInputdata,const void * inputdata);
CRYPTOLIB_API bool CRYPTOLIB_CALL clPKIVerifySignature(const char * pkalgorithm,const char * publickeyfile,const char * passphrase,size32_t lenSignature,const void * signature,size32_t lenSigneddata,const void * signeddata);

CRYPTOLIB_API void CRYPTOLIB_CALL clPKIEncrypt2(size32_t & __lenResult,void * & __result,const char * pkalgorithm,const char * publickeyfile,size32_t lenPassPhrase,const void * passphrase,size32_t lenInputdata,const void * inputdata);
CRYPTOLIB_API void CRYPTOLIB_CALL clPKIDecrypt2(size32_t & __lenResult,void * & __result,const char * pkalgorithm,const char * privatekeyfile,size32_t lenPassPhrase,const void * passphrase,size32_t lenEncrypteddata,const void * encrypteddata);

CRYPTOLIB_API void CRYPTOLIB_CALL clPKISign2(size32_t & __lenResult,void * & __result,const char * pkalgorithm,const char * privatekeyfile,size32_t lenPassPhrase,const void * passphrase,size32_t lenInputdata,const void * inputdata);
CRYPTOLIB_API bool CRYPTOLIB_CALL clPKIVerifySignature2(const char * pkalgorithm,const char * publickeyfile,size32_t lenPassPhrase,const void * passphrase,size32_t lenSignature,const void * signature,size32_t lenSigneddata,const void * signeddata);

//from LFN
CRYPTOLIB_API void CRYPTOLIB_CALL clPKIEncryptLFN(ICodeContext * ctx, size32_t & __lenResult,void * & __result,const char * pkalgorithm,const char * publickeyLFN,const char * passphrase,size32_t lenInputdata,const void * inputdata);
CRYPTOLIB_API void CRYPTOLIB_CALL clPKIDecryptLFN(ICodeContext * ctx, size32_t & __lenResult,void * & __result,const char * pkalgorithm,const char * privatekeyLFN,const char * passphrase,size32_t lenEncrypteddata,const void * encrypteddata);

CRYPTOLIB_API void CRYPTOLIB_CALL clPKISignLFN(ICodeContext * ctx, size32_t & __lenResult,void * & __result,const char * pkalgorithm,const char * privatekeyLFN,const char * passphrase,size32_t lenInputdata,const void * inputdata);
CRYPTOLIB_API bool CRYPTOLIB_CALL clPKIVerifySignatureLFN(ICodeContext * ctx, const char * pkalgorithm,const char * publickeyLFN,const char * passphrase,size32_t lenSignature,const void * signature,size32_t lenSigneddata,const void * signeddata);

CRYPTOLIB_API void CRYPTOLIB_CALL clPKIEncryptLFN2(ICodeContext * ctx, size32_t & __lenResult,void * & __result,const char * pkalgorithm,const char * publickeyLFN,size32_t lenPassPhrase,const void * passphrase,size32_t lenInputdata,const void * inputdata);
CRYPTOLIB_API void CRYPTOLIB_CALL clPKIDecryptLFN2(ICodeContext * ctx, size32_t & __lenResult,void * & __result,const char * pkalgorithm,const char * privatekeyLFN,size32_t lenPassPhrase,const void * passphrase,size32_t lenEncrypteddata,const void * encrypteddata);

CRYPTOLIB_API void CRYPTOLIB_CALL clPKISignLFN2(ICodeContext * ctx, size32_t & __lenResult,void * & __result,const char * pkalgorithm,const char * privatekeyLFN,size32_t lenPassPhrase,const void * passphrase,size32_t lenInputdata,const void * inputdata);
CRYPTOLIB_API bool CRYPTOLIB_CALL clPKIVerifySignatureLFN2(ICodeContext * ctx, const char * pkalgorithm,const char * publickeyLFN,size32_t lenPassPhrase,const void * passphrase,size32_t lenSignature,const void * signature,size32_t lenSigneddata,const void * signeddata);

//from buffer
CRYPTOLIB_API void CRYPTOLIB_CALL clPKIEncryptBuff(size32_t & __lenResult,void * & __result,const char * pkalgorithm,const char * publickeybuff,const char * passphrase,size32_t lenInputdata,const void * inputdata);
CRYPTOLIB_API void CRYPTOLIB_CALL clPKIDecryptBuff(size32_t & __lenResult,void * & __result,const char * pkalgorithm,const char * privatekeybuff,const char * passphrase,size32_t lenEncrypteddata,const void * encrypteddata);

CRYPTOLIB_API void CRYPTOLIB_CALL clPKISignBuff(size32_t & __lenResult,void * & __result,const char * pkalgorithm,const char * privatekeybuff,const char * passphrase,size32_t lenInputdata,const void * inputdata);
CRYPTOLIB_API bool CRYPTOLIB_CALL clPKIVerifySignatureBuff(const char * pkalgorithm,const char * publickeybuff,const char * passphrase,size32_t lenSignature,const void * signature,size32_t lenSigneddata,const void * signeddata);

CRYPTOLIB_API void CRYPTOLIB_CALL clPKIEncryptBuff2(size32_t & __lenResult,void * & __result,const char * pkalgorithm,const char * publickeybuff,size32_t lenPassPhrase,const void * passphrase,size32_t lenInputdata,const void * inputdata);
CRYPTOLIB_API void CRYPTOLIB_CALL clPKIDecryptBuff2(size32_t & __lenResult,void * & __result,const char * pkalgorithm,const char * privatekeybuff,size32_t lenPassPhrase,const void * passphrase,size32_t lenEncrypteddata,const void * encrypteddata);

CRYPTOLIB_API void CRYPTOLIB_CALL clPKISignBuff2(size32_t & __lenResult,void * & __result,const char * pkalgorithm,const char * privatekeybuff,size32_t lenPassPhrase,const void * passphrase,size32_t lenInputdata,const void * inputdata);
CRYPTOLIB_API bool CRYPTOLIB_CALL clPKIVerifySignatureBuff2(const char * pkalgorithm,const char * publickeybuff,size32_t lenPassPhrase,const void * passphrase,size32_t lenSignature,const void * signature,size32_t lenSigneddata,const void * signeddata);

}
#endif
