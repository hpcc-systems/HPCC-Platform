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
 * digisign regression tests
 *
 */

#ifdef _USE_CPPUNIT

#include "unittests.hpp"
#include "digisign.hpp"

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

class DigiSignUnitTest : public CppUnit::TestFixture
{
public:
    CPPUNIT_TEST_SUITE(DigiSignUnitTest);
        CPPUNIT_TEST(testSimple);
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
                bool ok = dsm->digiSign(text, sig);
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
                bool ok = dsm->digiVerify(text, sig);
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

                ok = dsm->digiVerify(text, sig);
                if (!ok)
                    printf("Asynchronous asyncDigiSignAndVerifyUnitTest() test %d failed!\n", idx);
                ASSERT(ok);
            }
        } afor(_dsm);

        printf("Executing 1000 asynchronous asyncDigiSignAndVerifyUnitTest() operations\n");
        afor.For(1000,20,true,true);
        printf("Asynchronous asyncDigiSignAndVerifyUnitTest() test complete\n");
}

    void testSimple()
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
            StringBuffer _pubKeyBuff(pubKey);
            StringBuffer _privKeyBuff(privKey);
            Owned<IDigitalSignatureManager> dsm(createDigitalSignatureManagerInstanceFromKeys(_pubKeyBuff, _privKeyBuff, nullptr));


            printf("digiSign() test 1\n");
            StringBuffer txt(text1);
            bool ok = dsm->digiSign(text1, sig1.clear());
            ASSERT(ok);
            ASSERT(0 == strcmp(text1, txt.str()));//source string should be unchanged
            ASSERT(!sig1.isEmpty());//signature should be populated

            StringBuffer sig(sig1);
            ok = dsm->digiVerify(text1, sig1);
            ASSERT(ok);
            ASSERT(0 == strcmp(text1, txt.str()));//source string should be unchanged
            ASSERT(0 == strcmp(sig.str(), sig1.str()));//signature should be unchanged

            printf("digiSign() test 2\n");
            ok = dsm->digiVerify(text1, sig1);
            ASSERT(ok);
            ok = dsm->digiVerify(text1, sig1);
            ASSERT(ok);

            printf("digiSign() test 3\n");
            ok = dsm->digiSign(text2, sig2.clear());
            ASSERT(ok);
            ok = dsm->digiVerify(text2, sig2);
            ASSERT(ok);
            ok = dsm->digiSign(text2, sig2.clear());
            ASSERT(ok);
            ok = dsm->digiVerify(text2, sig2);
            ASSERT(ok);

            printf("digiSign() test 4\n");
            ok = dsm->digiVerify(text1, sig1);
            ASSERT(ok);

            printf("digiSign() test 5\n");
            ok = dsm->digiVerify(text2, sig2);
            ASSERT(ok);

            printf("digiSign() test 6\n");
            ok = dsm->digiVerify(text1, sig2);
            ASSERT(!ok);//should fail

            printf("digiSign() test 7\n");
            ok = dsm->digiVerify(text2, sig1);
            ASSERT(!ok);//should fail

            printf("digiSign() test 8\n");
            ok = dsm->digiSign(text3, sig3.clear());
            ASSERT(ok);

            printf("digiSign() test 9\n");
            ok = dsm->digiVerify(text3, sig1);
            ASSERT(!ok);//should fail
            ok = dsm->digiVerify(text3, sig2);
            ASSERT(!ok);//should fail
            ok = dsm->digiVerify(text3, sig3);
            ASSERT(ok);

            //Perform
            printf("digiSign() loop test\n");
            unsigned now = msTick();
            for (int x=0; x<1000; x++)
            {
                dsm->digiSign(text3, sig3.clear());
            }
            printf("digiSign() 1000 iterations took %d MS\n", msTick() - now);

            printf("digiVerify() loop test\n");
            now = msTick();
            for (int x=0; x<1000; x++)
            {
                dsm->digiVerify(text3, sig3);
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
};

CPPUNIT_TEST_SUITE_REGISTRATION( DigiSignUnitTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( DigiSignUnitTest, "DigiSignUnitTest" );

#endif // _USE_CPPUNIT
