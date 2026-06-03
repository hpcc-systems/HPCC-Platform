#include "platform.h"
#include <time.h>
#include <stdlib.h>
#include <string.h>
#include "dmetaphone.hpp"
#include "metaphone.h"

#define DMETAPHONE_VERSION "DMETAPHONE 1.1.05"

static const char * compatibleVersions[] = {
    "DMETAPHONE 1.1.05 [0e64c86ec1d5771d4ce0abe488a98a2a]",
    "DMETAPHONE 1.1.05",
    NULL };

DMETAPHONE_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb)
{
    if (pb->size == sizeof(ECLPluginDefinitionBlockEx))
    {
        ECLPluginDefinitionBlockEx * pbx = (ECLPluginDefinitionBlockEx *) pb;
        pbx->compatibleVersions = compatibleVersions;
    }
    else if (pb->size != sizeof(ECLPluginDefinitionBlock))
        return false;

    pb->magicVersion = PLUGIN_VERSION;
    pb->version = DMETAPHONE_VERSION;
    pb->moduleName = "lib_metaphone";
    pb->ECL = NULL;  // Definition is in lib_metaphone.ecllib
    pb->flags = PLUGIN_IMPLICIT_MODULE;
    pb->description = "Metaphone library";
    return true;
}

namespace nsDmetaphone {

IPluginContext * parentCtx = NULL;

}

using namespace nsDmetaphone;

DMETAPHONE_API void setPluginContext(IPluginContext * _ctx) { parentCtx = _ctx; }


DMETAPHONE_API void DMETAPHONE_CALL mpDMetaphone1(size32_t & __ret_len,char * & __ret_str,unsigned _len_instr,const char * instr)
{
    cString metaph;
    cString metaph2;
    MString ms;
    ms.Set(instr, _len_instr);
    ms.DoubleMetaphone(metaph, metaph2);
    __ret_len = strlen((char*) metaph);
    __ret_str = (char *) CTXMALLOC(parentCtx, __ret_len+1);
    strcpy(__ret_str, (char*) metaph);
}

DMETAPHONE_API void DMETAPHONE_CALL mpDMetaphone2(size32_t & __ret_len,char * & __ret_str,unsigned _len_instr,const char * instr)
{
    cString metaph;
    cString metaph2;
    MString ms;
    ms.Set(instr, _len_instr);
    ms.DoubleMetaphone(metaph, metaph2);
    __ret_len = strlen((char*) metaph2);
    __ret_str = (char *) CTXMALLOC(parentCtx, __ret_len+1);
    strcpy(__ret_str, (char*) metaph2);
}

DMETAPHONE_API void DMETAPHONE_CALL mpDMetaphoneBoth(size32_t & __ret_len,char * & __ret_str,unsigned _len_instr,const char * instr)
{
    cString metaph;
    cString metaph2;
    MString ms;
    ms.Set(instr, _len_instr);
    ms.DoubleMetaphone(metaph, metaph2);
    __ret_len = strlen((char*) metaph) + strlen((char*) metaph2);
    __ret_str = (char *) CTXMALLOC(parentCtx, __ret_len+1);
    strcpy(__ret_str, (char*) metaph);
    strcat(__ret_str, (char*) metaph2);
}

DMETAPHONE_API void DMETAPHONE_CALL mpDMetaphone1_20(char * __ret_str,unsigned _len_instr,const char * instr)
{
    cString metaph;
    cString metaph2;
    MString ms;
    ms.Set(instr, _len_instr);
    ms.DoubleMetaphone(metaph, metaph2);
    memset(__ret_str, ' ', 20);
    size32_t metaph_len = strlen((char*) metaph);
    strncpy(__ret_str, (char*) metaph, (metaph_len > 20)?20:metaph_len);
}

DMETAPHONE_API void DMETAPHONE_CALL mpDMetaphone2_20(char * __ret_str,unsigned _len_instr,const char * instr)
{
    cString metaph;
    cString metaph2;
    MString ms;
    ms.Set(instr, _len_instr);
    ms.DoubleMetaphone(metaph, metaph2);
    memset(__ret_str, ' ', 20);
    size32_t metaph2_len = strlen((char*) metaph2);
    strncpy(__ret_str, (char*) metaph2, (metaph2_len > 20)?20:metaph2_len);
}

DMETAPHONE_API void DMETAPHONE_CALL mpDMetaphoneBoth_40(char * __ret_str,unsigned _len_instr,const char * instr)
{
    cString metaph;
    cString metaph2;
    MString ms;
    ms.Set(instr, _len_instr);
    ms.DoubleMetaphone(metaph, metaph2);
    memset(__ret_str, ' ', 40);
    size32_t metaph_len = strlen((char*) metaph);
    strncpy(__ret_str, (char*) metaph, (metaph_len > 20)?20:metaph_len);
    size32_t metaph2_len = strlen((char*) metaph2);
    strncpy(__ret_str+metaph_len, (char*) metaph2, (metaph2_len > 20)?20:metaph2_len);
}

#ifdef _USE_CPPUNIT
#include "unittests.hpp"
#include "jlib.hpp"
#include "jlog.hpp"
#include "jdebug.hpp"

struct TestCase
{
    std::string input;
    std::string expected1;
    std::string expected2;
};

static const TestCase testCases[] = {
    { "testing", "TSTNK", "TSTNK" },
    { "The", "0", "T" },
    { "quick", "KK", "KK" },
    { "brown", "PRN", "PRN" },
    { "fox", "FKS", "FKS" },
    { "jumped", "JMPT", "AMPT" },
    { "over", "AFR", "AFR" },
    { "the", "0", "T" },
    { "lazy", "LS", "LS" },
    { "dogs", "TKS", "TKS" },
    { "MacCafferey", "MKFR", "MKFR" },
    { "Stephan", "STFN", "STFN" },
    { "Kuczewski", "KSSK", "KXFSK" },
    { "McClelland", "MKLLNT", "MKLLNT" },
    { "san jose", "SNHS", "SNHS" },
    { "xenophobia", "SNFP", "SNFP" },
    { "Fokker", "FKR", "FKR" },
    { "Joqqi", "JK", "AK" },
    { "Hovvi", "HF", "HF" },
    { "Czerny", "SRN", "XRN" },
    { "Wasserman", "ASRMN", "FSRMN" },
    { "Breslin", "PRSLN", "PRSLN" },
    { "Szymanski", "SMNSK", "XMNSK" },
    { "Phelps", "FLPS", "FLPS" },
    { "Rousseau", "RS", "RS" },
    { "Pellegrini", "PLKRN", "PLKRN" },
    { "Smith", "SM0", "XMT" },
    { "Schmidt", "XMT", "SMT" },
    { "John", "JN", "AN" },
    { "O'Neill", "ANL", "ANL" },
    { "Zimmerman", "SMRMN", "SMRMN" },
    { "Dijkstra", "TKSTR", "TKSTR" },
    { "Wawrzyniak", "ARSNK", "FRTSNK" },
    { "Washington", "AXNKTN", "FXNKTN" },
    { "Xavier", "SF", "SFR" },
    { "Yoshi", "AX", "AX" },
    { "Queen", "KN", "KN" },
    { "Tchaikovsky", "XKFSK", "XKFSK" },
    { "Gough", "KF", "KF" },
    { "Hughes", "HS", "HS" },
    { "Laugh", "LF", "LF" },
    { "Cough", "KF", "KF" },
    { "Rough", "RF", "RF" },
    { "Tough", "TF", "TF" },
    { "Bough", "P", "P" },
    { "Through", "0R", "TR" },
    { "Although", "AL0", "ALT" },
    { "Dough", "T", "T" },
    { "Wombagh", "AMP", "FMP" },
    { "Slough", "SLF", "XLF" },
    { "Carlisle", "KRLL", "KRLL" },
    { "Carlyle", "KRLL", "KRLL" },
    { "Chiragh", "XR", "XR" },
    { "Ghilghit", "JLKT", "JLKT" },
    { "Gogh", "KK", "KK" },
    { "Hugh", "H", "H" },
    { "Slainte", "SLNT", "XLNT" },
    { "Gillespie", "KLSP", "JLSP" },
    { "Gilly", "KL", "JL" },
    { "Gila", "KL", "JL" },
    { "Agghzy", "AKS", "AKS" },
    { "Cagney", "KKN", "KKN" },
    { "Clough", "KLF", "KLF" },
    { "Halgh", "HLK", "HLK" },
    { "MacHugh", "MK", "MK" },
    { "McCullough", "MKLF", "MKLF" },
    { "MacLaughlin", "MKLFLN", "MKLFLN" },
    { "Taghert", "TKRT", "TKRT" },
    { "Vaughan", "FKN", "FKN" },
    { "Vaughan-Williams", "FKNLMS", "FKNLMS" },
    { "Aragh", "ARK", "ARK" },
    { "Creagh", "KRK", "KRK" },
    { "Darragh", "TRK", "TRK" },
    { "Farrell", "FRL", "FRL" },
    { "Gallagher", "KLKR", "KLKR" },
    { "Cavanagh", "KFNK", "KFNK" },
    { "Kavanagh", "KFNK", "KFNK" },
    { "Callaghan", "KLKN", "KLKN" },

    // Additional Edge Cases tested specifically for MString mapping
    // rules present in metaphone.cpp algorithms:
    { "Gnostic", "NSTK", "NSTK" },
    { "Pneumatic", "NMTK", "NMTK" },
    { "Wrong", "RNK", "RNK" },
    { "Psychology", "SXLJ", "SKLK" },
    { "dumb", "TM", "TM" },
    { "thumb", "0M", "TM" },
    { "CAESAR", "SSR", "SSR" },
    { "Chianti", "KNT", "KNT" },
    { "michael", "MKL", "MXL" },
    { "chemistry", "KMSTR", "KMSTR" },
    { "chorus", "KRS", "KRS" },
    { "architect", "ARKTKT", "ARKTKT" },
    { "orchestra", "ARKSTR", "ARKSTR" },
    { "orchid", "ARKT", "ARKT" },
    { "WICZ", "ATS", "FFX" },
    { "focaccia", "FKX", "FKX" },
    { "bellocchio", "PLX", "PLX" },
    { "bacchus", "PKS", "PKS" },
    { "accident", "AKSTNT", "AKSTNT" },
    { "accede", "AKST", "AKST" },
    { "succeed", "SKST", "SKST" },
    { "bacci", "PX", "PX" },
    { "bertucci", "PRTX", "PRTX" },
    { "edge", "AJ", "AJ" },
    { "edgar", "ATKR", "ATKR" },
    { "ghislane", "JLN", "JLN" },
    { "ghiradelli", "JRTL", "JRTL" },
    { "broughton", "PRTN", "PRTN" },
    { "tagliaro", "TKLR", "TLR" },
    { "danger", "TNJR", "TNKR" },
    { "ranger", "RNJR", "RNKR" },
    { "manger", "MNJR", "MNKR" },
    { "biaggi", "PJ", "PK" },
    { "cabrillo", "KPRL", "KPR" },
    { "gallegos", "KLKS", "KKS" },
    { "campbell", "KMPL", "KMPL" },
    { "raspberry", "RSPR", "RSPR" },
    { "rogier", "RJ", "RJR" },
    { "hochmeier", "HKMR", "HKMR" },
    { "island", "ALNT", "ALNT" },
    { "isle", "AL", "AL" },
    { "carlysle", "KRLL", "KRLL" },
    { "sugar", "XKR", "SKR" },
    { "school", "SKL", "SKL" },
    { "schooner", "SKNR", "SKNR" },
    { "schermerhorn", "XRMRRN", "SKRMRRN" },
    { "schenker", "XNKR", "SKNKR" },
    { "resnais", "RSN", "RSNS" },
    { "artois", "ART", "ARTS" },
    { "thomas", "TMS", "TMS" },
    { "thames", "TMS", "TMS" },
    { "Arnow", "ARN", "ARNF" },
    { "Arnoff", "ARNF", "ARNF" },
    { "filipowicz", "FLPTS", "FLPFX" },
    { "breaux", "PR", "PR" },
    { "zhao", "J", "J" },
    { "", "", "" }
};
// Test cases sources:
// Part of the data mapping is based on validations from Apache Commons Codec
// (Apache License 2.0).
// See org/apache/commons/codec/language/DoubleMetaphoneTest.java for their
// referenced initial dataset sources (ASPell).

class DMetaphoneTestBase
{
protected:
    void checkMetaphone(const TestCase & tc)
    {
        size32_t len1 = 0;
        size32_t len2 = 0;
        size32_t lenBoth = 0;
        char * str1 = nullptr;
        char * str2 = nullptr;
        char * strBoth = nullptr;

        try
        {
            mpDMetaphone1(len1, str1, tc.input.length(), tc.input.c_str());
            mpDMetaphone2(len2, str2, tc.input.length(), tc.input.c_str());
            mpDMetaphoneBoth(lenBoth, strBoth, tc.input.length(), tc.input.c_str());

            if ((tc.expected1.length() != len1) || memcmp(str1, tc.expected1.c_str(), len1) != 0)
                CPPUNIT_ASSERT_EQUAL(tc.expected1, std::string(str1, len1));
            if ((tc.expected2.length() != len2) || memcmp(str2, tc.expected2.c_str(), len2) != 0)
                CPPUNIT_ASSERT_EQUAL(tc.expected2, std::string(str2, len2));

            if (lenBoth != len1 + len2 || memcmp(strBoth, str1, len1) != 0 || memcmp(strBoth + len1, str2, len2) != 0)
            {
                CPPUNIT_ASSERT_EQUAL(len1 + len2, lenBoth);
                CPPUNIT_ASSERT(memcmp(strBoth, str1, len1) == 0);
                CPPUNIT_ASSERT(memcmp(strBoth + len1, str2, len2) == 0);
            }
        }
        catch (...)
        {
            CTXFREE(parentCtx, str1);
            CTXFREE(parentCtx, str2);
            CTXFREE(parentCtx, strBoth);
            throw;
        }

        CTXFREE(parentCtx, str1);
        CTXFREE(parentCtx, str2);
        CTXFREE(parentCtx, strBoth);
    }

public:
    void testExamples()
    {
        START_TEST

        for (unsigned i=0; i < _elements_in(testCases); i++)
            checkMetaphone(testCases[i]);

        END_TEST
    }
};

class DMetaphoneTest : public CppUnit::TestFixture, public DMetaphoneTestBase
{
    CPPUNIT_TEST_SUITE(DMetaphoneTest);
    CPPUNIT_TEST(testExamples);
    CPPUNIT_TEST_SUITE_END();
};

class DMetaphoneTimingTest : public CppUnit::TestFixture, public DMetaphoneTestBase
{
    CPPUNIT_TEST_SUITE(DMetaphoneTimingTest);
    CPPUNIT_TEST(testTiming);
    CPPUNIT_TEST_SUITE_END();

public:
    void testTiming()
    {
        START_TEST

        const unsigned iterations = 10000;
        CCycleTimer timer;
        for (unsigned i = 0; i < iterations; i++)
        {
            testExamples();
        }
        DBGLOG("DMetaphone timing test: %u iterations took %u ms", iterations, timer.elapsedMs());

        END_TEST
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(DMetaphoneTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(DMetaphoneTest, "DMetaphoneTest");

CPPUNIT_TEST_SUITE_REGISTRATION(DMetaphoneTimingTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(DMetaphoneTimingTest, "DMetaphoneTimingTest");

#endif
