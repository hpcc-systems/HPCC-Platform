/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

/*
 * cryptohelper regression tests
 *
 */

#ifdef _USE_CPPUNIT

#include <functional>

#include "jencrypt.hpp"

#include "unittests.hpp"
#include "digisign.hpp"
#include "pke.hpp"
#include "ske.hpp"


/* ============================================================= */

const char * privKey =
"-----BEGIN RSA PRIVATE KEY-----\n"
"MIIEpAIBAAKCAQEA1SISNsSwg81TClnjGtVV61cYOmGWgKbbnpvSOqbZWMHs9W4L\n"
"g2waNbGnQ3G+l7alWiMRV4qTjhkrdVRuvTxLOGOAfAhGB4Y5txDUNdTwuSp+Gpuq\n"
"coYOmQWW4IIrjIOZMlamZQcIs7p/2CzfoLQHNuFuBeR+MLDMsMO7O42N+pcFWqPC\n"
"plmRIkDWB2ru+DpJcaTtts/16f1/nf4KbMmpVlObWP/l48/XZjythzQir5AV6W13\n"
"VM7MFToQqPxjy/c9F06/RjiW7sFv/r58pNsPk0iWcL0wBJ0GHZRGsCNOKMl08qow\n"
"jynIVMKhIADYFXm84r69R1CO9KocixnqsH29uQIDAQABAoIBAEMCdFGN46V837fo\n"
"bPPZ0Sqt9msclZIbY/9pJF7WaI10Y0kC8VG/ojnxghI9Z9wRS8mcLu6kHiJWHYjF\n"
"JBARLeErv5C/lSz2cZzyCJZoPcsp5f39pUheh6Zq0HYD1ydVlMvz3Fr1LDI918Yi\n"
"zaicEYyasdnebiJm4+RLlclyhwoa8CeRNLbLRoHcL7mu7sHHDMIWS86P/axfAnZ4\n"
"yk2DKqjFflgd1zmRW5JLj6phb80ehuFIMJQ/Llwm4LY3uvg11D8c8ZDXQnMVSAIE\n"
"fV5X9dtS1LCexYIpRmLj/LTAYZbQSdmE2w2lXLnDewiFD57eJNjYK9O0+iZg/Nfj\n"
"i/95tVUCgYEA+D7N/LmWil3n/jz6zmrZPj+j7fjiZ+YiJ1mIOROvnlHhOgGzzjV5\n"
"hFAVET9vlqSQoOelK9aYVEl9yfi9fRq1TUGLucS9+x5Urt1FBWyJw5cgdkpUIY4k\n"
"pa9CCvnKrOieL+Rs4mU6XKqwx8iswv5PeOzW6/aMwbloVsAkYxEFiRsCgYEA28p6\n"
"JDMO0pJE3rmesyxpLMayGCtpiFhbhuoIsveae2Hf6Qe7Bg73FEMHeDnjgXzpN8IY\n"
"YgAMXglRsN09lPRc7cxUWdsr0slu8J/ouCaYu7l0i+Y1fp3YWLnUp56T45GGJPEI\n"
"ro6EIhyX2J7abFV5qNHzI+AnlPubL8XCzaUwNbsCgYEA2F/NpYGSAIq3Ynd+WJrz\n"
"Pfm0hgDQPqVtkYTNYoqRIVrXCHthYNRlVXmD02PKfLB1y3n9EsfaQGVKOdgQOdIk\n"
"wvDlvAcLXK1kPIJq3b5sGcpJJjHFQPYnZS7sTqrJCIs9Dht4+KApDYpNyeVVCCUn\n"
"2gv9jPB6YYScuDiDvsGgZI8CgYBUg1bT9I4Oig/RVK6hVsJaZUy13nuF4fPPvM37\n"
"gxnzt37RrBdODRMUx3Fn2VqRv+YteoTFqh8XSZ4P1AKJ9CyHg7orkwsW0j3GaLaj\n"
"mLPB+13FLZAET82Q0GPk0CUtrBdYvRYJiONl+nio4uw6G+Pb9l73vIl70AOsKu7t\n"
"BEe1YQKBgQDeW3xAP3ceoJAW5qfnTzCY20wfs/epVSczqm2ZBrLhnY2l0FgefXCg\n"
"eLLmQ8M0zCPrEIxsz11QmGunOVE0/boEOShYbOVTjAEES9a/+FHg0gyIP4zU8WZc\n"
"dye0XkCOCkcGIwdas3fwf+O0lwZc+dvPdKVak+e8qL9aPgiQCb2/ww==\n"
"-----END RSA PRIVATE KEY-----\n";

const char * pubKey =
"-----BEGIN PUBLIC KEY-----\n"
"MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA1SISNsSwg81TClnjGtVV\n"
"61cYOmGWgKbbnpvSOqbZWMHs9W4Lg2waNbGnQ3G+l7alWiMRV4qTjhkrdVRuvTxL\n"
"OGOAfAhGB4Y5txDUNdTwuSp+GpuqcoYOmQWW4IIrjIOZMlamZQcIs7p/2CzfoLQH\n"
"NuFuBeR+MLDMsMO7O42N+pcFWqPCplmRIkDWB2ru+DpJcaTtts/16f1/nf4KbMmp\n"
"VlObWP/l48/XZjythzQir5AV6W13VM7MFToQqPxjy/c9F06/RjiW7sFv/r58pNsP\n"
"k0iWcL0wBJ0GHZRGsCNOKMl08qowjynIVMKhIADYFXm84r69R1CO9KocixnqsH29\n"
"uQIDAQAB\n"
"-----END PUBLIC KEY-----\n";

/* ============================================================= */

/*Private key, with a passphrase, using
   openssl genrsa -aes128 -passout pass:ThisIsMyPassphrase -out priv.pem 1024
*/

const char * privKeyPassphrase =
"-----BEGIN RSA PRIVATE KEY-----\n"
"Proc-Type: 4,ENCRYPTED\n"
"DEK-Info: AES-128-CBC,27840180591F7545A3BC6AC26017B5E2\n"
"\n"
"JZ7kSTs0chmd3TmPTWQW3NM9dtfgJN59cecq8UzNeDfNdXQYU5WwhPFebpqX6K4H\n"
"hRJ4pKaFCS39+ib68Yalwb5T+vru9t6WHhJkbGcl41bz6U0aXs3FCEEGFUEngVWu\n"
"lonE8YjbeC+kiE7UlnGiFweteTJNlzbsFfa0w3U/6/tkfbd6ZDbriEhUvrbp1EPw\n"
"JAAZDs9MNCqs2S76VqqWHyWhVI32lgauVRqDNZTZDSnXF9/huUUSuK8fLK4G68Jz\n"
"0gSb7AeR9/AaJgg1FVUantmX7Ja60qLQW4O6DzTJgTGtuKEhaX3wNjpH5aKw8Ifn\n"
"gVdZrm9hBKGQCxC5JjVjcrKRXKjj7iKf+d0UN57q9BlKcqw+r+ET2Lqf2jnm1XTt\n"
"O1i6VkEGCZxSKdy2jb0d1kHNJonXyrW7mukfclO2LKqDwWYr2efu4wv0Dt9ttWeA\n"
"jL6taU7O3aGwjTibLW8qcneWKQogIwnmvY2TsDTtL7Pr+zXpIeBOvuu9+IEGV5nm\n"
"j4pVrlApKDF7+hhhYyevJSEfnImCwgeji3pZ5CnFEYASBMEGZmGmWtJyZ/sDkrTe\n"
"RjyOV22NaHWtu7HISaOgU/inG8NwGsOL91osnmE+hB07vr44Blaz2oHQhtEZb35k\n"
"YLeP+sf4MK0iQy/aKnLcZBHig8/m4MIPNKgpHu/MJ03pbiNiUzV34q4IkuvQiEFf\n"
"9N/p4HHRx6789Ndf1b8+iW0VtftfTt/HXYnSw2I1InFfB8KmnC20gYIQEorGPUuX\n"
"32yYXSjYNdyWZI52PwX57LD/A5YwkuTowib5MyYFoA2Po51B9bHNCTwzN1RTfGbH\n"
"-----END RSA PRIVATE KEY-----\n";

/*Public key generated using
   openssl rsa -in priv.pem -passin pass:ThisIsMyPassphrase -pubout -out pub.pem
*/

const char * pubKeyPassphrase =
"-----BEGIN PUBLIC KEY-----\n"
"MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCWnKkGM0l3Y6pKhxMq87hAGBL6\n"
"FfEo2HC6XCSQuaAMLkdf7Yjn3FpvFIEO6A1ZYJy70cT8+HOFta+sSUyMn2fDc5cv\n"
"VdX8v7XCycYXEBeZ4KsTCHHPCUoO/nxNbxhNz09T8dx/JsIH50LHipR6FTLTSCXR\n"
"N9KVLaPXs5DdQx6PjQIDAQAB\n"
"-----END PUBLIC KEY-----\n";

/* ============================================================= */

using namespace cryptohelper;

#ifdef _USE_OPENSSL
class CryptoUnitTest : public CppUnit::TestFixture
{
public:
    CPPUNIT_TEST_SUITE(CryptoUnitTest);
        CPPUNIT_TEST(digiSignTests);
        CPPUNIT_TEST(pkeEncryptDecryptTest);
        CPPUNIT_TEST(pkeEncryptDecryptPassphraseTest);
        CPPUNIT_TEST(pkeParallelTest);
        CPPUNIT_TEST(aesEncryptDecryptTests);
        CPPUNIT_TEST(aesWithRsaEncryptedKey);
        CPPUNIT_TEST(aesParallelTest);
    CPPUNIT_TEST_SUITE_END();

protected:
    void asyncDigiSignUnitTest(IDigitalSignatureManager * _dsm)
    {

        class casyncfor: public CAsyncFor
        {
            IDigitalSignatureManager * dsm;
        public:
            casyncfor(IDigitalSignatureManager * _dsm)
            {
                dsm = _dsm;
            }
            void Do(unsigned idx)
            {
                VStringBuffer text("I am here %d", idx);
                StringBuffer sig;
                bool ok = dsm->digiSign(sig, text);
                if (!ok)
                    printf("Asynchronous asyncDigiSignUnitTest() test %d failed!\n", idx);
                ASSERT(ok);
            }
        } afor(_dsm);

        printf("Executing 1000 asyncDigiSignUnitTest() operations\n");
        afor.For(1000,20,true,true);
        printf("Asynchronous asyncDigiSignUnitTest() test complete\n");
    }

    void asyncDigiVerifyUnitTest(IDigitalSignatureManager * _dsm)
    {

        class casyncfor: public CAsyncFor
        {
            IDigitalSignatureManager * dsm;
            StringBuffer text;
            StringBuffer sig;
        public:
            casyncfor(IDigitalSignatureManager * _dsm)
            {
                dsm = _dsm;
                text.set("I am here");
                bool ok = dsm->digiSign(text, sig);
                if (!ok)
                    printf("Asynchronous asyncDigiVerifyUnitTest() failed in digiSign!\n");
                ASSERT(ok);
            }
            void Do(unsigned idx)
            {
                bool ok = dsm->digiVerify(sig, text);
                if (!ok)
                    printf("Asynchronous asyncDigiVerifyUnitTest() test %d failed!\n", idx);
                ASSERT(ok);
            }
        } afor(_dsm);

        printf("Executing 1000 asyncDigiVerifyUnitTest() operations\n");
        afor.For(1000,20,true,true);
        printf("Asynchronous asyncDigiVerifyUnitTest() test complete\n");
    }

    void asyncDigiSignAndVerifyUnitTest(IDigitalSignatureManager * _dsm)
    {

        class casyncfor: public CAsyncFor
        {
            IDigitalSignatureManager * dsm;
        public:
            casyncfor(IDigitalSignatureManager * _dsm)
            {
                dsm = _dsm;
            }
            void Do(unsigned idx)
            {
                VStringBuffer text("I am here %d", idx);
                StringBuffer sig;
                bool ok = dsm->digiSign(text, sig);
                if (!ok)
                    printf("Asynchronous asyncDigiSignAndVerifyUnitTest() test %d failed!\n", idx);
                ASSERT(ok);

                ok = dsm->digiVerify(sig, text);
                if (!ok)
                    printf("Asynchronous asyncDigiSignAndVerifyUnitTest() test %d failed!\n", idx);
                ASSERT(ok);
            }
        } afor(_dsm);

        printf("Executing 1000 asynchronous asyncDigiSignAndVerifyUnitTest() operations\n");
        afor.For(1000,20,true,true);
        printf("Asynchronous asyncDigiSignAndVerifyUnitTest() test complete\n");
    }

    void digiSignTests()
    {
        Owned<IException> exception;
        CppUnit::Exception *cppunitException;

        const char * text1 = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        const char * text2 = "~`!@#$%^&*()_-+=0123456789{[}]:;\"'<,>.?/'";
        const char * text3 = "W20180301-154415;ECLUsername";
        StringBuffer sig1;
        StringBuffer sig2;
        StringBuffer sig3;

        try
        {
            printf("\nExecuting digiSign() unit tests\n");

            //Create instance of digital signature manager
            Owned<IDigitalSignatureManager> dsm(createDigitalSignatureManagerInstanceFromKeys(pubKey, privKey, nullptr));

            printf("digiSign() test 1\n");
            StringBuffer txt(text1);
            bool ok = dsm->digiSign(sig1.clear(), text1);
            ASSERT(ok);
            ASSERT(0 == strcmp(text1, txt.str()));//source string should be unchanged
            ASSERT(!sig1.isEmpty());//signature should be populated

            StringBuffer sig(sig1);
            ok = dsm->digiVerify(sig1, text1);
            ASSERT(ok);
            ASSERT(0 == strcmp(text1, txt.str()));//source string should be unchanged
            ASSERT(0 == strcmp(sig.str(), sig1.str()));//signature should be unchanged

            printf("digiSign() test 2\n");
            ok = dsm->digiVerify(sig1, text1);
            ASSERT(ok);
            ok = dsm->digiVerify(sig1, text1);
            ASSERT(ok);

            printf("digiSign() test 3\n");
            ok = dsm->digiSign(sig2.clear(), text2);
            ASSERT(ok);
            ok = dsm->digiVerify(sig2, text2);
            ASSERT(ok);
            ok = dsm->digiSign(sig2.clear(), text2);
            ASSERT(ok);
            ok = dsm->digiVerify(sig2, text2);
            ASSERT(ok);

            printf("digiSign() test 4\n");
            ok = dsm->digiVerify(sig1, text1);
            ASSERT(ok);

            printf("digiSign() test 5\n");
            ok = dsm->digiVerify(sig2, text2);
            ASSERT(ok);

            printf("digiSign() test 6\n");
            ok = dsm->digiVerify(sig2, text1);
            ASSERT(!ok);//should fail

            printf("digiSign() test 7\n");
            ok = dsm->digiVerify(sig1, text2);
            ASSERT(!ok);//should fail

            printf("digiSign() test 8\n");
            ok = dsm->digiSign(sig3.clear(), text3);
            ASSERT(ok);

            printf("digiSign() test 9\n");
            ok = dsm->digiVerify(sig1, text3);
            ASSERT(!ok);//should fail
            ok = dsm->digiVerify(sig2, text3);
            ASSERT(!ok);//should fail
            ok = dsm->digiVerify(sig3, text3);
            ASSERT(ok);

            //Perform
            printf("digiSign() loop test\n");
            unsigned now = msTick();
            for (int x=0; x<1000; x++)
            {
                dsm->digiSign(sig3.clear(), text3);
            }
            printf("digiSign() 1000 iterations took %d MS\n", msTick() - now);

            printf("digiVerify() loop test\n");
            now = msTick();
            for (int x=0; x<1000; x++)
            {
                dsm->digiVerify(sig3, text3);
            }
            printf("digiverify 1000 iterations took %d MS\n", msTick() - now);

            now = msTick();
            printf("\nAsynchronous test digiSign\n");
            asyncDigiSignUnitTest(dsm);
            printf("digiSign 1000 async iterations took %d MS\n", msTick() - now);

            now = msTick();
            printf("\nAsynchronous test digiVerify\n");
            asyncDigiVerifyUnitTest(dsm);
            printf("digiverify 1000 async iterations took %d MS\n", msTick() - now);

            now = msTick();
            printf("\nAsynchronous test digiSign and digiVerify\n");
            asyncDigiSignAndVerifyUnitTest(dsm);
            printf("digiSign/digiverify 1000 async iterations took %d MS\n", msTick() - now);
        }
        catch (IException *e)
        {
            StringBuffer err;
            e->errorMessage(err);
            printf("Digisign IException thrown:%s\n", err.str());
            exception.setown(e);
        }
        catch (CppUnit::Exception &e)
        {
            printf("Digisign CppUnit::Exception thrown\n");
            cppunitException = e.clone();
        }
        printf("Completed executing digiSign() unit tests\n");
    }

    void _pkeEncryptDecryptTest()
    {
        try
        {
            Owned<CLoadedKey> publicKey = loadPublicKeyFromMemory(pubKey);
            Owned<CLoadedKey> privateKey = loadPrivateKeyFromMemory(privKey, nullptr);

            // create random data
            MemoryBuffer toEncryptMb;
            fillRandomData(245, toEncryptMb); // max for RSA

            MemoryBuffer pkeMb;
            publicKeyEncrypt(pkeMb, toEncryptMb.length(), toEncryptMb.bytes(), *publicKey);

            MemoryBuffer decryptedMb;
            privateKeyDecrypt(decryptedMb, pkeMb.length(), pkeMb.bytes(), *privateKey);

            ASSERT(toEncryptMb.length() == decryptedMb.length());
            ASSERT(0 == memcmp(toEncryptMb.bytes(), decryptedMb.bytes(), toEncryptMb.length()));
        }
        catch (IException *e)
        {
            StringBuffer err;
            e->errorMessage(err);
            printf("pkeEncryptDecryptTest IException thrown:%s\n", err.str());
            throw;
        }
        catch (CppUnit::Exception &e)
        {
            printf("pkeEncryptDecryptTest CppUnit::Exception thrown\n");
            throw;
        }
    }

    void pkeEncryptDecryptTest()
    {
        printf("\nExecuting pkeEncryptDecryptTest() unit tests\n");
        _pkeEncryptDecryptTest();
    }

    void _pkeEncryptDecryptPassphraseTest()
    {
        try
        {
            // create random data
            MemoryBuffer toEncryptMb;
            fillRandomData(64, toEncryptMb);

            MemoryBuffer pkeMb;
            MemoryBuffer decryptedMb;

            /////////////////////////////////////
            //PKE tests using a passphrase string
            /////////////////////////////////////

            Owned<CLoadedKey> publicKey = loadPublicKeyFromMemory(pubKeyPassphrase);
            Owned<CLoadedKey> privateKey = loadPrivateKeyFromMemory(privKeyPassphrase, "ThisIsMyPassphrase");

            publicKeyEncrypt(pkeMb, toEncryptMb.length(), toEncryptMb.bytes(), *publicKey);
            privateKeyDecrypt(decryptedMb, pkeMb.length(), pkeMb.bytes(), *privateKey);

            ASSERT(toEncryptMb.length() == decryptedMb.length());
            ASSERT(0 == memcmp(toEncryptMb.bytes(), decryptedMb.bytes(), toEncryptMb.length()));

            /////////////////////////////////////
            //PKE tests using a passphrase buffer
            /////////////////////////////////////

            Owned<CLoadedKey> publicKeyPassphrase = loadPublicKeyFromMemory(pubKeyPassphrase);
            Owned<CLoadedKey> privateKeyPassphrase = loadPrivateKeyFromMemory(privKeyPassphrase, 18, "ThisIsMyPassphrase");

            pkeMb.clear();
            publicKeyEncrypt(pkeMb, toEncryptMb.length(), toEncryptMb.bytes(), *publicKeyPassphrase);
            decryptedMb.clear();
            privateKeyDecrypt(decryptedMb, pkeMb.length(), pkeMb.bytes(), *privateKeyPassphrase);

            ASSERT(toEncryptMb.length() == decryptedMb.length());
            ASSERT(0 == memcmp(toEncryptMb.bytes(), decryptedMb.bytes(), toEncryptMb.length()));

        }
        catch (IException *e)
        {
            StringBuffer err;
            e->errorMessage(err);
            printf("pkeEncryptDecryptPassphraseTest IException thrown:%s\n", err.str());
            throw;
        }
        catch (CppUnit::Exception &e)
        {
            printf("pkeEncryptDecryptPassphraseTest CppUnit::Exception thrown\n");
            throw;
        }
    }

    void pkeEncryptDecryptPassphraseTest()
    {
        printf("\nExecuting pkeEncryptDecryptPassphraseTest() unit tests\n");
        _pkeEncryptDecryptPassphraseTest();
    }

    void pkeParallelTest()
    {
        class CAsyncfor : public CAsyncFor
        {
            std::function<void()> testFunc;
        public:
            CAsyncfor(std::function<void()> _testFunc) : testFunc(_testFunc)
            {
            }
            void Do(unsigned idx)
            {
                testFunc();
            }
        } afor(std::bind(&CryptoUnitTest::_pkeEncryptDecryptTest, this));

        printf("\nExecuting 1000 asynchronous pkeParallelTest() operations\n");
        CCycleTimer timer;
        afor.For(1000, 20, true, true);
        printf("Asynchronous pkeParallelTest() test completed in %u ms\n", timer.elapsedMs());
    }

    void aesEncryptDecryptTests()
    {
        try
        {
            printf("\nExecuting aesEncryptDecryptTests() unit tests\n");
            // create random data
            MemoryBuffer messageMb, encryptedMessageMb, decryptedMessageMb;

            char aesKey[aesMaxKeySize];
            char aesIV[aesBlockSize];
            fillRandomData(aesMaxKeySize, aesKey);
            fillRandomData(aesBlockSize, aesIV);

            fillRandomData(1024*100, messageMb);
            printf("aesEncryptDecryptTests with %u bytes with 256bit aes key\n", messageMb.length());
            aesEncrypt(encryptedMessageMb, messageMb.length(), messageMb.bytes(), aesMaxKeySize, aesKey, aesIV);
            aesDecrypt(decryptedMessageMb, encryptedMessageMb.length(), encryptedMessageMb.bytes(), aesMaxKeySize, aesKey, aesIV);
            ASSERT(messageMb.length() == decryptedMessageMb.length());
            ASSERT(0 == memcmp(messageMb.bytes(), decryptedMessageMb.bytes(), messageMb.length()));

            printf("aesEncryptDecryptTests with %u bytes with 192bit aes key\n", messageMb.length());
            aesEncrypt(encryptedMessageMb.clear(), messageMb.length(), messageMb.bytes(), 192/8, aesKey, aesIV);
            aesDecrypt(decryptedMessageMb.clear(), encryptedMessageMb.length(), encryptedMessageMb.bytes(), 192/8, aesKey, aesIV);
            ASSERT(messageMb.length() == decryptedMessageMb.length());
            ASSERT(0 == memcmp(messageMb.bytes(), decryptedMessageMb.bytes(), messageMb.length()));

            printf("aesEncryptDecryptTests with %u bytes with 128bit aes key\n", messageMb.length());
            aesEncrypt(encryptedMessageMb.clear(), messageMb.length(), messageMb.bytes(), 128/8, aesKey, aesIV);
            aesDecrypt(decryptedMessageMb.clear(), encryptedMessageMb.length(), encryptedMessageMb.bytes(), 128/8, aesKey, aesIV);
            ASSERT(messageMb.length() == decryptedMessageMb.length());
            ASSERT(0 == memcmp(messageMb.bytes(), decryptedMessageMb.bytes(), messageMb.length()));

            messageMb.clear(); // 0 length test
            printf("aesEncryptDecryptTests with %u bytes\n", messageMb.length());
            aesEncrypt(encryptedMessageMb.clear(), messageMb.length(), messageMb.bytes(), aesMaxKeySize, aesKey, aesIV);
            aesDecrypt(decryptedMessageMb.clear(), encryptedMessageMb.length(), encryptedMessageMb.bytes(), aesMaxKeySize, aesKey, aesIV);
            ASSERT(messageMb.length() == decryptedMessageMb.length());

            fillRandomData(1, messageMb.clear()); // 1 byte test
            printf("aesEncryptDecryptTests with %u bytes\n", messageMb.length());
            aesEncrypt(encryptedMessageMb.clear(), messageMb.length(), messageMb.bytes(), aesMaxKeySize, aesKey, aesIV);
            aesDecrypt(decryptedMessageMb.clear(), encryptedMessageMb.length(), encryptedMessageMb.bytes(), aesMaxKeySize, aesKey, aesIV);
            ASSERT(messageMb.length() == decryptedMessageMb.length());
            ASSERT(0 == memcmp(messageMb.bytes(), decryptedMessageMb.bytes(), messageMb.length()));

            fillRandomData(cryptohelper::aesBlockSize-1, messageMb.clear()); // aesBlockSize-1 test
            printf("aesEncryptDecryptTests with %u bytes\n", messageMb.length());
            aesEncrypt(encryptedMessageMb.clear(), messageMb.length(), messageMb.bytes(), aesMaxKeySize, aesKey, aesIV);
            aesDecrypt(decryptedMessageMb.clear(), encryptedMessageMb.length(), encryptedMessageMb.bytes(), aesMaxKeySize, aesKey, aesIV);
            ASSERT(messageMb.length() == decryptedMessageMb.length());
            ASSERT(0 == memcmp(messageMb.bytes(), decryptedMessageMb.bytes(), messageMb.length()));

            fillRandomData(cryptohelper::aesBlockSize, messageMb.clear()); // aesBlockSize test
            printf("aesEncryptDecryptTests with %u bytes\n", messageMb.length());
            aesEncrypt(encryptedMessageMb.clear(), messageMb.length(), messageMb.bytes(), aesMaxKeySize, aesKey, aesIV);
            aesDecrypt(decryptedMessageMb.clear(), encryptedMessageMb.length(), encryptedMessageMb.bytes(), aesMaxKeySize, aesKey, aesIV);
            ASSERT(messageMb.length() == decryptedMessageMb.length());
            ASSERT(0 == memcmp(messageMb.bytes(), decryptedMessageMb.bytes(), messageMb.length()));

            fillRandomData(cryptohelper::aesBlockSize+1, messageMb.clear()); // aesBlockSize+1 test
            printf("aesEncryptDecryptTests with %u bytes\n", messageMb.length());
            aesEncrypt(encryptedMessageMb.clear(), messageMb.length(), messageMb.bytes(), aesMaxKeySize, aesKey, aesIV);
            aesDecrypt(decryptedMessageMb.clear(), encryptedMessageMb.length(), encryptedMessageMb.bytes(), aesMaxKeySize, aesKey, aesIV);
            ASSERT(messageMb.length() == decryptedMessageMb.length());
            ASSERT(0 == memcmp(messageMb.bytes(), decryptedMessageMb.bytes(), messageMb.length()));
        }
        catch (IException *e)
        {
            StringBuffer err;
            e->errorMessage(err);
            printf("aesWithRsaEncryptedKey IException thrown:%s\n", err.str());
            throw;
        }
        catch (CppUnit::Exception &e)
        {
            printf("aesWithRsaEncryptedKey CppUnit::Exception thrown\n");
            throw;
        }
    }

    void aesWithRsaEncryptedKey()
    {
        try
        {
            printf("\nExecuting aesWithRsaEncryptedKey() unit tests\n");
            // create random data
            MemoryBuffer messageMb;
            fillRandomData(1024*100, messageMb);

            char aesKey[aesMaxKeySize];
            char aesIV[aesBlockSize];
            fillRandomData(aesMaxKeySize, aesKey);
            fillRandomData(aesBlockSize, aesIV);

            Owned<CLoadedKey> publicKey = loadPublicKeyFromMemory(pubKey);
            MemoryBuffer encryptedMessageMb;
            aesEncryptWithRSAEncryptedKey(encryptedMessageMb, messageMb.length(), messageMb.bytes(), *publicKey);

            // would normally be server side
            Owned<CLoadedKey> privateKey = loadPrivateKeyFromMemory(privKey, nullptr);
            MemoryBuffer decryptedMessageMb;
            aesDecryptWithRSAEncryptedKey(decryptedMessageMb, encryptedMessageMb.length(), encryptedMessageMb.bytes(), *privateKey);

            ASSERT(messageMb.length() == decryptedMessageMb.length());
            ASSERT(0 == memcmp(messageMb.bytes(), decryptedMessageMb.bytes(), messageMb.length()));
        }
        catch (IException *e)
        {
            StringBuffer err;
            e->errorMessage(err);
            printf("aesWithRsaEncryptedKey IException thrown:%s\n", err.str());
            throw;
        }
        catch (CppUnit::Exception &e)
        {
            printf("aesWithRsaEncryptedKey CppUnit::Exception thrown\n");
            throw;
        }
    }

    void aesParallelTest()
    {
        class CAsyncfor : public CAsyncFor
        {
            MemoryBuffer messageMb;
            char aesKey[aesMaxKeySize];
            char aesIV[aesBlockSize];
        public:
            CAsyncfor()
            {
                // create random key
                fillRandomData(aesMaxKeySize, aesKey);
                fillRandomData(aesBlockSize, aesIV);
                // create random data
                fillRandomData(1024*100, messageMb);
            }
            void Do(unsigned idx)
            {
                MemoryBuffer encryptedMessageMb;
                aesEncrypt(encryptedMessageMb, messageMb.length(), messageMb.bytes(), aesMaxKeySize, aesKey, aesIV);

                MemoryBuffer decryptedMessageMb;
                aesDecrypt(decryptedMessageMb, encryptedMessageMb.length(), encryptedMessageMb.bytes(), aesMaxKeySize, aesKey, aesIV);

                ASSERT(messageMb.length() == decryptedMessageMb.length());
                ASSERT(0 == memcmp(messageMb.bytes(), decryptedMessageMb.bytes(), messageMb.length()));
            }
        } afor;

        printf("\nExecuting 1000 asynchronous aesParallelTest() operations\n");
        CCycleTimer timer;
        afor.For(1000, 20, true, true);
        printf("Asynchronous aesParallelTest() test completed in %u ms\n", timer.elapsedMs());
    }
};

class CryptoTestTiming : public CppUnit::TestFixture
{
    size32_t dataSz = 0x100000 * 10; // 10MB

public:
    CPPUNIT_TEST_SUITE(CryptoTestTiming);
        CPPUNIT_TEST(aesSpeedTest);
        CPPUNIT_TEST(rsaSpeedTest);
        CPPUNIT_TEST(rsaKeyLoadSpeedTest);
        CPPUNIT_TEST(aesCompareJlibVsCryptoHelper);
    CPPUNIT_TEST_SUITE_END();

    void aesCompareJlibVsCryptoHelper()
    {
        MemoryBuffer messageMb, encryptedMessageMb, decryptedMessageMb;
        char aesKey[aesMaxKeySize];
        char aesIV[aesBlockSize];
        // create random key
        fillRandomData(aesMaxKeySize, aesKey);
        fillRandomData(aesBlockSize, aesIV);

        // create random data
        fillRandomData(dataSz, messageMb);

        encryptedMessageMb.ensureCapacity(dataSz+aesBlockSize);

        CCycleTimer timer;
        cryptohelper::aesEncrypt(encryptedMessageMb, messageMb.length(), messageMb.bytes(), aesMaxKeySize, aesKey, aesIV);
        printf("OPENSSL AES %u MB encrypt time: %u ms\n", dataSz/0x100000, timer.elapsedMs());

        decryptedMessageMb.ensureCapacity(encryptedMessageMb.length()+aesBlockSize);
        timer.reset();
        cryptohelper::aesDecrypt(decryptedMessageMb, encryptedMessageMb.length(), encryptedMessageMb.bytes(), aesMaxKeySize, aesKey, aesIV);
        printf("OPENSSL AES %u MB decrypt time: %u ms\n", dataSz/0x100000, timer.elapsedMs());

        ASSERT(messageMb.length() == decryptedMessageMb.length());
        ASSERT(0 == memcmp(messageMb.bytes(), decryptedMessageMb.bytes(), messageMb.length()));

        encryptedMessageMb.clear();
        timer.reset();
        jlib::aesEncrypt(aesKey, aesMaxKeySize, messageMb.bytes(), messageMb.length(), encryptedMessageMb);
        printf("JLIB    AES %u MB encrypt time: %u ms\n", dataSz/0x100000, timer.elapsedMs());

        decryptedMessageMb.clear();
        timer.reset();
        jlib::aesDecrypt(aesKey, aesMaxKeySize, encryptedMessageMb.bytes(), encryptedMessageMb.length(), decryptedMessageMb);
        printf("JLIB    AES %u MB decrypt time: %u ms\n", dataSz/0x100000, timer.elapsedMs());

        ASSERT(messageMb.length() == decryptedMessageMb.length());
        ASSERT(0 == memcmp(messageMb.bytes(), decryptedMessageMb.bytes(), messageMb.length()));
    }

    void aesSpeedTest()
    {
        MemoryBuffer messageMb;
        char aesKey[aesMaxKeySize];
        char aesIV[aesBlockSize];
        // create random key
        fillRandomData(aesMaxKeySize, aesKey);
        fillRandomData(aesBlockSize, aesIV);

        // create random data
        fillRandomData(dataSz, messageMb);

        MemoryBuffer encryptedMessageMb;
        encryptedMessageMb.ensureCapacity(dataSz+aesBlockSize);
        CCycleTimer timer;
        aesEncrypt(encryptedMessageMb, messageMb.length(), messageMb.bytes(), aesMaxKeySize, aesKey, aesIV);
        printf("AES %u MB encrypt time: %u ms\n", dataSz/0x100000, timer.elapsedMs());
        MemoryBuffer decryptedMessageMb;
        decryptedMessageMb.ensureCapacity(encryptedMessageMb.length()+aesBlockSize);
        timer.reset();
        aesDecrypt(decryptedMessageMb, encryptedMessageMb.length(), encryptedMessageMb.bytes(), aesMaxKeySize, aesKey, aesIV);
        printf("AES %u MB decrypt time: %u ms\n", dataSz/0x100000, timer.elapsedMs());
    }

    void rsaSpeedTest()
    {
        // create random data
        MemoryBuffer messageMb;
        fillRandomData(dataSz, messageMb);

        Owned<CLoadedKey> publicKey = loadPublicKeyFromMemory(pubKey);
        Owned<CLoadedKey> privateKey = loadPrivateKeyFromMemory(privKey, nullptr);

        MemoryBuffer encryptedMessageMb;
        MemoryBuffer decryptedMessageMb;
        // pre-alloc memory, so as not part of the timing
        size32_t maxPerEncryptSz = 245;
        unsigned numPackets = ((dataSz + (maxPerEncryptSz-1)) / maxPerEncryptSz);
        size32_t dstMaxSz = numPackets * 256; // approx
        encryptedMessageMb.ensureCapacity(dstMaxSz);

        const byte *src = messageMb.bytes();
        byte *dst = (byte *)encryptedMessageMb.bufferBase();
        size32_t remaining = dataSz;
        size32_t encryptPacketSz = 256;
        CCycleTimer timer;
        while (true)
        {
            size32_t cp = remaining>maxPerEncryptSz ? maxPerEncryptSz : remaining;
            size_t eSz = publicKeyEncrypt(dst, dstMaxSz, cp, src, *publicKey);
            assertex(eSz);
            assertex(eSz == encryptPacketSz); //consistent for size being encrypted, assumed on decrypt
            src += cp;
            remaining -= cp;
            dst += eSz;
            dstMaxSz -= eSz;
            if (0 == remaining)
                break;
        }
        encryptedMessageMb.rewrite(dst-(byte*)encryptedMessageMb.bufferBase());
        printf("RSA %u MB encrypt time: %u ms\n", dataSz/0x100000, timer.elapsedMs());

        size32_t encryptedDataSz = encryptedMessageMb.length();
        remaining = encryptedDataSz;
        src = encryptedMessageMb.bytes();
        dstMaxSz = ((numPackets-1) * maxPerEncryptSz) + encryptPacketSz; // because encrypt always needs buffer to have enough room for encryptPacketSz
        decryptedMessageMb.ensureCapacity(dstMaxSz);
        dst = (byte *)decryptedMessageMb.bufferBase();
        timer.reset();
        while (true)
        {
            size_t eSz = privateKeyDecrypt(dst, dstMaxSz, encryptPacketSz, src, *privateKey);
            assertex(eSz);
            assertex(eSz <= maxPerEncryptSz);
            src += encryptPacketSz;
            remaining -= encryptPacketSz;
            dst += eSz;
            dstMaxSz -= eSz;
            if (0 == remaining)
                break;
        }
        printf("RSA %u MB decrypt time: %u ms\n", dataSz/0x100000, timer.elapsedMs());
    }

    void rsaKeyLoadSpeedTest()
    {
        // create random data
        size32_t dataSz = 245;
        MemoryBuffer messageMb;
        fillRandomData(dataSz, messageMb);

        unsigned numCycles = 1000;
        CCycleTimer timer;
        for (unsigned i=0; i<numCycles; i++)
        {
            Owned<CLoadedKey> publicKey = loadPublicKeyFromMemory(pubKey);
            Owned<CLoadedKey> privateKey = loadPrivateKeyFromMemory(privKey, nullptr);

            MemoryBuffer pkeMb;
            publicKeyEncrypt(pkeMb, messageMb.length(), messageMb.bytes(), *publicKey);

            MemoryBuffer decryptedMb;
            privateKeyDecrypt(decryptedMb, pkeMb.length(), pkeMb.bytes(), *privateKey);
        }
        printf("RSA %u cycles - reloading keys each iteration - %u ms\n", numCycles, timer.elapsedMs());

        Owned<CLoadedKey> publicKey = loadPublicKeyFromMemory(pubKey);
        Owned<CLoadedKey> privateKey = loadPrivateKeyFromMemory(privKey, nullptr);

        timer.reset();
        for (unsigned i=0; i<numCycles; i++)
        {
            MemoryBuffer pkeMb;
            publicKeyEncrypt(pkeMb, messageMb.length(), messageMb.bytes(), *publicKey);

            MemoryBuffer decryptedMb;
            privateKeyDecrypt(decryptedMb, pkeMb.length(), pkeMb.bytes(), *privateKey);
        }
        printf("RSA %u cycles - reusing loaded keys - %u ms\n", numCycles, timer.elapsedMs());
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( CryptoUnitTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( CryptoUnitTest, "CryptoUnitTest" );
CPPUNIT_TEST_SUITE_REGISTRATION( CryptoTestTiming );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( CryptoTestTiming, "CryptoTestTiming" );

#endif

#endif // _USE_CPPUNIT
