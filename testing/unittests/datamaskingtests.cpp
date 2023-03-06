/*##############################################################################

    Copyright (C) 2022 HPCC Systems®.

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

#ifdef _USE_CPPUNIT
#include "datamaskingplugin.hpp"
#include "datamaskingengine.hpp"
#include "datamaskingcompatibility.hpp"
#include "unittests.hpp"
#include <vector>

struct SubstituteException
{
};
struct ExpectedException : public SubstituteException
{
};

ITracer* createTracer()
{
  class Filter : public CInterfaceOf<IModularTraceMsgFilter>
  {
  public:
      virtual bool rejects(const LogMsgCategory& category) const override
      {
        return ((category.queryClass() & classMask) != category.queryClass());
      }
    private:
      int classMask = MSGCLS_error;
    public:
      Filter()
      {
      }
      Filter(int _classMask)
        : classMask(_classMask)
      {
      }
  };
  Owned<CModularTracer> tracer(new CModularTracer());
  tracer->setSink(new CConsoleTraceMsgSink(true, true));
  tracer->setFilter(new Filter());
  //tracer->setFilter(new Filter(MSGCLS_error | MSGCLS_information));
  return tracer.getLink();
}

class DataMaskingEngineTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( DataMaskingEngineTests );
        CPPUNIT_TEST(testMinimalPlugin);
        CPPUNIT_TEST(testLegacyDomains);
        CPPUNIT_TEST(testMultipleDomains);
        CPPUNIT_TEST(testCheckCompatibility);
    CPPUNIT_TEST_SUITE_END();

    using Engine = DataMasking::CEngine;
    using Checker = DataMasking::CCompatibilityChecker;

    struct CurrentTest
    {
      DataMaskingEngineTests& tester;
      CurrentTest(DataMaskingEngineTests& _tester, const char* test)
          : tester(_tester)
      {
        tester.currentTest = test;
      }
      ~CurrentTest()
      {
        tester.currentTest = nullptr;
      }
    };
    const char* currentTest = nullptr;

public:
    void testMinimalPlugin()
    {
      static const char* cfgTxt = R"!!!(
        maskingPlugin:
        - library: datamasker
          entryPoint: newPartialMaskSerialToken
          domain: 'urn:hpcc:unittest'
      )!!!";

      CurrentTest ct(*this, "testMinimalPlugin");
      Owned<Engine> engine(createEngine(cfgTxt));

      assertDomainCount(engine, 1);
      assertHasDomain(engine, "urn:hpcc:unittest", true);
      assertHasDomain(engine, "bad", false);
      assertGetDomains(engine, {"urn:hpcc:unittest" });
      assertGetDomainIds(engine, "urn:hpcc:unittest", { "urn:hpcc:unittest" });
      assertDomainVersions(engine, "urn:hpcc:unittest", { 1 });
      assertQueryProfile(engine, "", 0, true);
    }

    void testLegacyDomains()
    {
      static const char* cfgTxt = R"!!!(
        maskingPlugin:
          library: datamasker
          entryPoint: newPartialMaskSerialToken
          profile:
          - domain: mydomain
            maximumVersion: 1
          - domain: myreviseddomain
            minimumVersion: 2
            maximumVersion: 3
            legacyDomain:
            - id: mydomain
          - domain: 'urn:hpcc:unittest'
            minimumVersion: 4
            legacyDomain:
            - id: myreviseddomain
            - id: mydomain
      )!!!";

      CurrentTest ct(*this, "testLegacyDomains");
      Owned<Engine> engine(createEngine(cfgTxt));

      assertDomainCount(engine, 1);
      assertHasDomain(engine, "mydomain", true);
      assertHasDomain(engine, "myreviseddomain", true);
      assertHasDomain(engine, "urn:hpcc:unittest", true);
      assertGetDomains(engine, { "urn:hpcc:unittest" });
      assertGetDomainIds(engine, "urn:hpcc:unittest", { "mydomain", "myreviseddomain", "urn:hpcc:unittest" });
      assertDomainVersions(engine, "urn:hpcc:unittest", { 1, 2, 3, 4 });
      assertQueriedProfile(engine, nullptr, 0, "urn:hpcc:unittest", 4);
      assertQueriedProfile(engine, nullptr, 1, "mydomain", 1);
      assertQueriedProfile(engine, nullptr, 2, "myreviseddomain", 2);
      assertQueriedProfile(engine, nullptr, 3, "myreviseddomain", 3);
      assertQueriedProfile(engine, nullptr, 4, "urn:hpcc:unittest", 4);
      assertQueriedProfile(engine, "mydomain", 0, "urn:hpcc:unittest", 4);
      assertQueriedProfile(engine, "mydomain", 1, "mydomain", 1);
      assertQueriedProfile(engine, "mydomain", 2, "myreviseddomain", 2);
      assertQueriedProfile(engine, "mydomain", 3, "myreviseddomain", 3);
      assertQueriedProfile(engine, "mydomain", 4, "urn:hpcc:unittest", 4);
      assertQueriedProfile(engine, "myreviseddomain", 0, "urn:hpcc:unittest", 4);
      assertQueriedProfile(engine, "myreviseddomain", 1, "mydomain", 1);
      assertQueriedProfile(engine, "myreviseddomain", 2, "myreviseddomain", 2);
      assertQueriedProfile(engine, "myreviseddomain", 3, "myreviseddomain", 3);
      assertQueriedProfile(engine, "myreviseddomain", 4, "urn:hpcc:unittest", 4);
      assertQueriedProfile(engine, "urn:hpcc:unittest", 0, "urn:hpcc:unittest", 4);
      assertQueriedProfile(engine, "urn:hpcc:unittest", 1, "mydomain", 1);
      assertQueriedProfile(engine, "urn:hpcc:unittest", 2, "myreviseddomain", 2);
      assertQueriedProfile(engine, "urn:hpcc:unittest", 3, "myreviseddomain", 3);
      assertQueriedProfile(engine, "urn:hpcc:unittest", 4, "urn:hpcc:unittest", 4);
    }

    void testMultipleDomains()
    {
      static const char* cfgTxt = R"!!!(
        maskingPlugin:
          library: datamasker
          entryPoint: newPartialMaskSerialToken
          profile:
          - domain: 'urn:hpcc:unittest:us'
          - domain: 'urn:hpcc:unittest:uk'
      )!!!";

      CurrentTest ct(*this, "testMultipleDomains");
      Owned<Engine> engine(createEngine(cfgTxt));

      assertDomainCount(engine, 2);
      assertHasDomain(engine, "urn:hpcc:unittest:us", true);
      assertHasDomain(engine, "urn:hpcc:unittest:uk", true);
      assertDomainVersions(engine, "urn:hpcc:unittest:us", { 1 });
      assertDomainVersions(engine, "urn:hpcc:unittest:uk", { 1 });
      assertQueryProfile(engine, "urn:hpcc:unittest:us", 0, true);
      assertQueryProfile(engine, "urn:hpcc:unittest:uk", 0, true);
      assertQueryProfile(engine, nullptr, 0, true);
    }

    void testCheckCompatibility()
    {
      static const char* cfgTxt = R"!!!(
        maskingPlugin:
          library: datamasker
          entryPoint: newPartialMaskSerialToken
          profile:
          - domain: 'urn:hpcc:unittest'
            property:
            - name: required-acceptance
            valueType:
            - name: secret
            - name: secret-if-a
              memberOf:
              - name: value-type-set-a
              maskStyle:
              - name: keep-last-4-numbers
                action: keep
                location: last
                characters: numbers
                count: 4
              rule:
              - startToken: <bar>
                endToken: </bar>
            - name: secret-if-b
              memberOf:
              - name: value-type-set-b
              rule:
              - contentType: xml
                startToken: <baz>
                endToken: </baz>
              - contentType: xml
                startToken: <baz2>
                endToken: </baz2>
                memberOf:
                - name: rule-set-2
            - name: '*'
      )!!!";
      static const char* chkTxt = R"!!!(
          - compatibility:
              context:
                domain: 'urn:hpcc:unittest'
                version: 0
                property:
                - name: valuetype-set
                  value: '*'
                - name: rule-set
                  value: '*'
              operation:
              - name: maskValue
                presence: r
              - name: maskContent
                presence: r
              - name: maskMarkupValue
                presence: o
              accepts:
              - name: valuetype-set
                presence: r
              - name: 'valuetype-set:value-type-set-a'
                presence: r
              - name: 'valuetype-set:value-type-set-b'
                presence: r
              - name: rule-set
                presence: r
              - name: 'rule-set:rule-set-2'
                presence: r
              - name: required-acceptance
                presence: r
              - name: optional-acceptance
                presence: o
              uses:
              - name: valuetype-set
                presence: r
              - name: 'valuetype-set:value-type-set-a'
                presence: r
              - name: 'valuetype-set:value-type-set-b'
                presence: r
              - name: rule-set
                presence: r
              - name: 'rule-set:rule-set-2'
                presence: r
              - name: required-acceptance
                presence: p
              - name: optional-acceptance
                presence: p
              valueType:
              - name: secret
                presence: r
              - name: secret-if-a
                presence: r
                maskStyle:
                - name: keep-last-4-numbers
                  presence: r
                - name: mask-last-4-numbers
                  presence: o
                Set:
                - name: value-type-set-a
                  presence: r
              - name: secret-if-b
                presence: r
                Set:
                - name: value-type-set-b
                  presence: r
              - name: '*'
                presence: r
              rule:
              - contentType: ''
                presence: r
              - contentType: xml
                presence: r
          - compatibility:
              context:
                domain: 'urn:hpcc:unittest'
                version: 0
                property:
                - name: valuetype-set
                  value: value-type-set-a
                - name: rule-set
                  value: ''
              valueType:
              - name: secret
                presence: r
              - name: secret-if-a
                presence: r
                maskStyle:
                - name: keep-last-4-numbers
                  presence: r
                - name: mask-last-4-numbers
                  presence: o
                Set:
                - name: value-type-set-a
                  presence: r
              - name: secret-if-b
                presence: p
                Set:
                - name: value-type-set-b
                  presence: r
              rule:
              - contentType: ''
                presence: r
              - contentType: xml
                presence: r
          - compatibility:
              context:
                domain: 'urn:hpcc:unittest'
                version: 0
                property:
                - name: valuetype-set
                  value: value-type-set-b
                - name: rule-set
                  value: ''
              valueType:
              - name: secret
                presence: r
              - name: secret-if-a
                presence: p
                maskStyle:
                - name: keep-last-4-numbers
                  presence: r
                - name: mask-last-4-numbers
                  presence: o
                Set:
                - name: value-type-set-a
                  presence: r
              - name: secret-if-b
                presence: r
                Set:
                - name: value-type-set-b
                  presence: r
              rule:
              - contentType: ''
                presence: r
              - contentType: xml
                presence: r
          - compatibility:
              context:
                domain: 'urn:hpcc:unittest'
                version: 0
                property:
                - name: valuetype-set
                - name: rule-set
                  value: ''
              valueType:
              - name: secret
                presence: r
              - name: secret-if-a
                presence: p
                maskStyle:
                - name: keep-last-4-numbers
                  presence: r
                - name: mask-last-4-numbers
                  presence: o
                Set:
                - name: value-type-set-a
                  presence: r
              - name: secret-if-b
                presence: p
                Set:
                - name: value-type-set-b
                  presence: r
              rule:
              - contentType: ''
                presence: p
              - contentType: xml
                presence: p
          - compatibility:
              context:
                domain: 'urn:hpcc:unittest'
                version: 0
                property:
                - name: valuetype-set
                - name: rule-set
                  value: 'rule-set-2'
              valueType:
              - name: secret
                presence: r
              - name: secret-if-a
                presence: p
                maskStyle:
                - name: keep-last-4-numbers
                  presence: r
                - name: mask-last-4-numbers
                  presence: o
                Set:
                - name: value-type-set-a
                  presence: r
              - name: secret-if-b
                presence: p
                Set:
                - name: value-type-set-b
                  presence: r
              rule:
              - contentType: ''
                presence: p
              - contentType: xml
                presence: p
          - compatibility:
              context:
                domain: 'urn:hpcc:unittest'
                version: 0
                property:
                - name: valuetype-set
                  value: value-type-set-b
                - name: rule-set
                  value: 'rule-set-2'
              valueType:
              - name: secret
                presence: r
              - name: secret-if-a
                presence: p
                maskStyle:
                - name: keep-last-4-numbers
                  presence: r
                - name: mask-last-4-numbers
                  presence: o
                Set:
                - name: value-type-set-a
                  presence: r
              - name: secret-if-b
                presence: r
                Set:
                - name: value-type-set-b
                  presence: r
              rule:
              - contentType: ''
                presence: r
              - contentType: xml
                presence: r
          - compatibility:
              context:
                domain: 'urn:hpcc:unittest'
                version: 0
                property:
                - name: valuetype-set
                - name: rule-set
                  value: 'rule-set-2'
              valueType:
              - name: secret
                presence: r
              - name: secret-if-a
                presence: p
                maskStyle:
                - name: keep-last-4-numbers
                  presence: r
                - name: mask-last-4-numbers
                  presence: o
                Set:
                - name: value-type-set-a
                  presence: r
              - name: secret-if-b
                presence: p
                Set:
                - name: value-type-set-b
                  presence: r
              rule:
              - contentType: ''
                presence: p
              - contentType: xml
                presence: p
      )!!!";

      CurrentTest ct(*this, "testCheckCompatibility");
      Owned<ITracer> tracer(createTracer());
      bool                  compatible = false;

      try
      {
        Owned<Engine>         engine(createEngine(cfgTxt, tracer));
        Checker               checker(*engine, tracer);
        Owned<IPTree>         reqs(createPTreeFromYAMLString(chkTxt));
        compatible = checker.checkCompatibility(*reqs);
      }
      catch (IException* e)
      {
        StringBuffer msg;
        tracer->ierrlog("%s: exception checking compatibility [%s]", currentTest, e->errorMessage(msg).str());
        e->Release();
      }
      catch (const std::exception& e)
      {
        tracer->ierrlog("%s: exception checking compatibility [%s]", currentTest, e.what());
      }
      catch (...)
      {
        tracer->ierrlog("%s: exception checking compatibility", currentTest);
      }
      if (!compatible)
      {
        CPPUNIT_ASSERT(false);
      }
    }

protected:
    IPTree* createConfiguration(const char* cfgTxt)
    {
      Owned<IPTree>  cfg;
      try
      {
        cfg.setown(createPTreeFromYAMLString(cfgTxt));
      }
      catch (IException *e)
      {
        StringBuffer msg;
        e->errorMessage(msg);
        fprintf(stdout, "\n%s: exception while parsing '%s' [%s]\n", currentTest, cfgTxt, msg.str());
        e->Release();
        throw SubstituteException();
      }
      return cfg.getClear();
    }

    Engine* createEngine(const char* cfgTxt)
    {
      return createEngine(cfgTxt, nullptr);
    }
    Engine* createEngine(const char* cfgTxt, ITracer* _tracer)
    {
      bool failed = false;
      Owned<ITracer> tracer(LINK(_tracer));
      if (!tracer)
          tracer.setown(createTracer());
      Owned<Engine> engine(new Engine(tracer.getLink()));
      Owned<IPTree> cfg(createConfiguration(cfgTxt));
      Owned<IPTreeIterator> it(cfg->getElements("//maskingPlugin"));
      ForEach(*it)
      {
        if (!engine->loadProfiles(it->query()))
        {
          fprintf(stdout, "\n%s: loadProfiles failed\n", currentTest);
          failed = true;
        }
      }
      CPPUNIT_ASSERT(!failed);
      return engine.getClear();
    }

    void assertDomainCount(Engine* engine, size_t expected)
    {
      size_t actual = engine->domainCount();
      if (actual != expected)
      {
        fprintf(stdout, "\n%s: domain count mismatch (%zu <> %zu)\n", currentTest, actual, expected);
        CPPUNIT_ASSERT(false);
      }
    }

    void assertHasDomain(Engine* engine, const char* id, bool expected)
    {
      bool actual = engine->hasDomain(id);
      if (actual != expected)
      {
        fprintf(stdout, "\n%s: hasDomain mismatch for '%s'\n", currentTest, id);
        CPPUNIT_ASSERT(false);
      }
    }

    void assertGetDomains(Engine* engine, const std::set<std::string>& expected)
    {
      assertTextIterator(engine->getDomains(), expected, "default domain identifier");
    }

    void assertGetDomainIds(Engine* engine, const char* domain, const std::set<std::string>& expected)
    {
      assertTextIterator(engine->getDomainIds(domain), expected, "domain");
    }

    void assertDomainVersions(Engine* engine, const char* domain, const std::set<uint8_t>& expected)
    {
      DataMaskingVersionCoverage actual;
      engine->getDomainVersions(domain, actual);
      if (expected.empty())
      {
        if (actual.any())
        {
          std::string actualText = actual.to_string();
          fprintf(stdout, "\n%s: unexpected coverage of domain '%s': %s\n", currentTest, domain, actualText.c_str());
          CPPUNIT_ASSERT(false);
        }
      }
      else
      {
        DataMaskingVersionCoverage expectedCoverage, missing, unexpected;
        for (uint8_t e : expected)
        {
          if (e)
            expectedCoverage.set(e);
        }
        missing = expectedCoverage;
        missing &= ~actual;
        unexpected = actual;
        unexpected &= ~expectedCoverage;
        if (missing.any() || unexpected.any())
        {
          if (missing.any())
          {
            std::string missingText = missing.to_string();
            fprintf(stdout, "\n%s: missing domain versions for '%s': %s\n", currentTest, domain, missingText.c_str());
          }
          if (unexpected.any())
          {
            std::string unexpectedText = unexpected.to_string();
            fprintf(stdout, "\n%s: unexpected domain versions for '%s': %s\n", currentTest, domain, unexpectedText.c_str());
          }
          CPPUNIT_ASSERT(false);
        }
      }
    }

    void assertTextIterator(ITextIterator* it, const std::set<std::string>& expectedValues, const char* label)
    {
      bool failed = false;
      Owned<ITextIterator> rawValues(it);
      std::set<std::string> actualValues;
      ForEach(*it)
      {
        std::string value(it->query());
        if (!actualValues.insert(value).second)
        {
          fprintf(stdout, "\n%s: duplicate actual %s '%s'\n", currentTest, label, value.c_str());
          failed = true;
        }
        else if (expectedValues.find(value) == expectedValues.end())
        {
          fprintf(stdout, "\n%s: unexpected actual %s '%s'\n", currentTest, label, value.c_str());
          failed = true;
        }
      }
      if (!checkStringSets(actualValues, expectedValues, label))
        failed = true;
      CPPUNIT_ASSERT(!failed);
    }

    bool checkStringSets(const std::set<std::string>& actual, const std::set<std::string>& expected, const char* label)
    {
      bool failed = false;
      if (expected != actual)
      {
        for (const std::string& v : expected)
        {
          if (actual.find(v) != actual.end())
          {
            fprintf(stdout, "\n%s: missing expected %s '%s'\n", currentTest, label, v.c_str());
            failed = true;
          }
        }
      }
      return !failed;
    }

    void assertQueryProfile(Engine* engine, const char* domain, uint8_t version, bool expected)
    {
      IDataMaskingProfile* profile = nullptr;
      try
      {
        profile = engine->queryProfile(domain, version);
      }
      catch (IException* e)
      {
        if (engine->domainCount() > 1 && isEmptyString(domain))
        {
          e->Release();
          throw ExpectedException();
        }
        throw;
      }
      if (profile)
      {
        if (!expected)
        {
          fprintf(stdout, "\n%s: unexpected profile for domain %s\n", currentTest, domain);
          CPPUNIT_ASSERT(false);
        }
        else
        {
          const IDataMaskerInspector& inspector = profile->inspector();
          if (!isEmptyString(domain) && !inspector.acceptsDomain(domain))
          {
            fprintf(stdout, "\n%s: domain mismatch for domain %s\n", currentTest, domain);
            CPPUNIT_ASSERT(false);
          }
          if (version && ((version < inspector.queryMinimumVersion()) || (inspector.queryMaximumVersion() < version)))
          {
            fprintf(stdout, "\n%s: version %hhu not in range %hhu..%hhu\n", currentTest, version, inspector.queryMinimumVersion(), inspector.queryMaximumVersion());
            CPPUNIT_ASSERT(false);
          }
        }
      }
      else
      {
        if (expected)
        {
          fprintf(stdout, "\n%s: didn't find profile for domain '%s' and version %hhu\n", currentTest, domain, version);
          CPPUNIT_ASSERT(false);
        }
      }
    }

    void assertQueriedProfile(Engine* engine, const char* requestedDomain, uint8_t requestedVersion, const char* expectedDomain, uint8_t expectedVersion)
    {
      IDataMaskingProfile* profile = engine->queryProfile(requestedDomain, requestedVersion);
      if (!profile)
      {
        fprintf(stdout, "\n%s: no profile match for '%s' version %hhu\n", currentTest, requestedDomain, requestedVersion);
        CPPUNIT_ASSERT(false);
      }
      const IDataMaskerInspector& inspector = profile->inspector();
      if (!streq(inspector.queryDefaultDomain(), expectedDomain) || (expectedVersion && (expectedVersion < inspector.queryMinimumVersion() || (inspector.queryMaximumVersion() < expectedVersion))))
      {
        fprintf(stdout, "\n%s: expected profile for '%s' version %hhu; got profile '%s' spanning %hhu..%hhu\n", currentTest, expectedDomain, expectedVersion, inspector.queryDefaultDomain(), inspector.queryMinimumVersion(), inspector.queryMaximumVersion());
        CPPUNIT_ASSERT(false);
      }
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( DataMaskingEngineTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( DataMaskingEngineTests, "datamaskingenginetests" );

class DataMaskingProfileTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( DataMaskingProfileTests );
        CPPUNIT_TEST(testMinimalProfile);
        CPPUNIT_TEST(testMinimalValueType);
        CPPUNIT_TEST(testPartialMaskStyle);
        CPPUNIT_TEST(testSerialTokenRule);
        CPPUNIT_TEST(testProperties);
        CPPUNIT_TEST(testVersioning);
        CPPUNIT_TEST(testUnconditional);
        CPPUNIT_TEST(testTraceOperation);
    CPPUNIT_TEST_SUITE_END();

    using MaskStyle = DataMasking::CPartialMaskStyle;
    using Rule = DataMasking::CSerialTokenRule;
    using ValueType = DataMasking::TValueType<MaskStyle, Rule>;
    using Context = DataMasking::CContext;
    using Profile = DataMasking::TSerialProfile<ValueType, Rule, Context>;
    using Plugin = DataMasking::TPlugin<Profile>;

    struct CurrentTest
    {
      DataMaskingProfileTests& tester;
      CurrentTest(DataMaskingProfileTests& _tester, const char* test)
          : tester(_tester)
      {
        tester.currentTest = test;
      }
      ~CurrentTest()
      {
        tester.currentTest = nullptr;
      }
    };
    const char* currentTest = nullptr;
public:
    void testMinimalProfile()
    {
      static const char* cfgTxt = R"!!!(
        profile:
          domain: 'urn:hpcc:unittest'
      )!!!";

      CurrentTest ct(*this, "testMinimalProfile");
      Owned<Profile> profile(createProfile(cfgTxt, true));

      assertDefaultDomain(profile, "urn:hpcc:unittest");
      assertAcceptedDomains(profile, { "urn:hpcc:unittest" });
      assertVersions(profile, 1, 1, 1);
      assertContexts(profile);
      Owned<IDataMaskingProfileContext> context(profile->createContext(0, nullptr));
      assertValueTypes(context, {});
      assertAcceptedProperties(context, {});
      assertUsedProperties(context, {});
      assertRules(context, nullptr, 0);
      assertMaskValue(context, "foo", "", "no mask", nullptr);
    }
    void testMinimalValueType()
    {
      static const char* cfgTxt = R"!!!(
        profile:
          domain: 'urn:hpcc:unittest'
          valueType:
            name: foo
      )!!!";

      CurrentTest ct(*this, "testMinimalValueType");
      Owned<IDataMaskingProfileContext> context(createContext(cfgTxt, 0));
      assertValueTypes(context, { "foo" });
      assertRules(context, nullptr, 0);
      assertMaskValue(context, "foo", "", "mask me", "*******");
    }
    void testPartialMaskStyle()
    {
      static const char* cfgTxt = R"!!!(
        profile:
          domain: 'urn:hpcc:unittest'
          valueType:
            name: foo
            maskStyle:
            - name: default-full
              overrideDefault: true
              pattern: x
            - name: full
              pattern: +-
            - name: default-partial
              count: 4
            - name: mask-first-numbers
              action: mask
              location: first
              characters: numbers
              count: 4
            - name: mask-first-letters
              action: mask
              location: first
              characters: letters
              count: 4
            - name: mask-first-alphanumeric
              action: mask
              location: first
              characters: alphanumeric
              count: 4
            - name: mask-first-characters
              action: mask
              location: first
              characters: all
              count: 4
            - name: mask-last-numbers
              action: mask
              location: last
              characters: numbers
              count: 4
            - name: mask-last-letters
              action: mask
              location: last
              characters: letters
              count: 4
            - name: mask-last-alphanumeric
              action: mask
              location: last
              characters: alphanumeric
              count: 4
            - name: mask-last-characters
              action: mask
              location: last
              characters: all
              count: 4
            - name: keep-first-numbers
              action: keep
              location: first
              characters: numbers
              count: 4
            - name: keep-first-letters
              action: keep
              location: first
              characters: letters
              count: 4
            - name: keep-first-alphanumeric
              action: keep
              location: first
              characters: alphanumeric
              count: 4
            - name: keep-first-characters
              action: keep
              location: first
              characters: all
              count: 4
            - name: keep-last-numbers
              action: keep
              location: last
              characters: numbers
              count: 4
            - name: keep-last-letters
              action: keep
              location: last
              characters: letters
              count: 4
            - name: keep-last-alphanumeric
              action: keep
              location: last
              characters: alphanumeric
              count: 4
            - name: keep-last-characters
              action: keep
              location: last
              characters: all
              count: 4
      )!!!";
      static const char* input = "!1a@2b#3c$4d%5e^6f&7g*8h(9i)";
      static const std::map<std::string, const char*> style2expected({
        { "",                        "xxxxxxxxxxxxxxxxxxxxxxxxxxxx" },
        { "default-full",            "xxxxxxxxxxxxxxxxxxxxxxxxxxxx" },
        { "full",                    "+-+-+-+-+-+-+-+-+-+-+-+-+-+-" },
        { "default-partial",         "!1a@2b#3c$4d%5e^6f&7g*8h****" },
        { "mask-first-numbers",      "***********d%5e^6f&7g*8h(9i)" },
        { "mask-first-letters",      "************%5e^6f&7g*8h(9i)" },
        { "mask-first-alphanumeric", "******#3c$4d%5e^6f&7g*8h(9i)" },
        { "mask-first-characters",   "****2b#3c$4d%5e^6f&7g*8h(9i)" },
        { "mask-last-numbers",       "!1a@2b#3c$4d%5e^************" },
        { "mask-last-letters",       "!1a@2b#3c$4d%5e^6***********" },
        { "mask-last-alphanumeric",  "!1a@2b#3c$4d%5e^6f&7g*******" },
        { "mask-last-characters",    "!1a@2b#3c$4d%5e^6f&7g*8h****" },
        { "keep-first-numbers",      "!1a@2b#3c$4*****************" },
        { "keep-first-letters",      "!1a@2b#3c$4d****************" },
        { "keep-first-alphanumeric", "!1a@2b**********************" },
        { "keep-first-characters",   "!1a@************************" },
        { "keep-last-numbers",       "****************6f&7g*8h(9i)" },
        { "keep-last-letters",       "*****************f&7g*8h(9i)" },
        { "keep-last-alphanumeric",  "**********************8h(9i)" },
        { "keep-last-characters",    "************************(9i)" },
      });
      static const std::set<std::string> styles({
        "default-full",
        "full",
        "default-partial",
        "mask-first-numbers",
        "mask-first-letters",
        "mask-first-alphanumeric",
        "mask-first-characters",
        "mask-last-numbers",
        "mask-last-letters",
        "mask-last-alphanumeric",
        "mask-last-characters",
        "keep-first-numbers",
        "keep-first-letters",
        "keep-first-alphanumeric",
        "keep-first-characters",
        "keep-last-numbers",
        "keep-last-letters",
        "keep-last-alphanumeric",
        "keep-last-characters",
      });

      CurrentTest ct(*this, "testPartialMaskStyle");
      Owned<IDataMaskingProfileContext> context(createContext(cfgTxt, 0));

      assertMaskStyles(context, "foo", styles);
      assertRules(context, nullptr, 0);
      for (std::map<std::string, const char*>::value_type vt : style2expected)
        assertMaskValue(context, "foo", vt.first.c_str(), input, vt.second);
    }
    void testSerialTokenRule()
    {
      static const char* cfgTxt = R"!!!(
        profile:
          domain: 'urn:hppc:unittest'
          valueType:
            name: foo
            rule:
            - startToken: <Mask>
              endToken: </Mask>
              contentType: xml
              matchCase: true
            - startToken: 'masked: '
            - contentType: unused
              startToken: <mask>
              endToken: </mask>
              matchCase: true
      )!!!";
      static const char* input = R"!!!(
        <Mask>mask me</Mask><NoMask>unmasked</NoMask><mask>oops</mask>
        Masked: don't forget me
      )!!!";
      static const char* expectedXml = R"!!!(
        <Mask>*******</Mask><NoMask>unmasked</NoMask><mask>oops</mask>
        Masked: ***************
      )!!!";
      static const char* expectedAll = R"!!!(
        <Mask>*******</Mask><NoMask>unmasked</NoMask><mask>****</mask>
        Masked: ***************
      )!!!";

      CurrentTest ct(*this, "testSerialTokenRule");
      Owned<IDataMaskingProfileContext> context(createContext(cfgTxt, 0));

      assertRules(context, "xml", 2);
      assertRules(context, nullptr, 3);
      assertRules(context, "unused", 2);
      assertMaskContent(context, "xml", input, expectedXml);
      assertMaskContent(context, nullptr, input, expectedAll);
    }
    void testProperties()
    {
      static const char* cfgTxt = R"!!!(
        profile:
          domain: 'urn:hpcc:unittest'
          property:
          - name: p1
          - name: p2
          - name: p3
          valueType:
          - name: foo
            memberOf:
            - name: vt1
            - name: vt2
            rule:
            - startToken: "st1: "
              memberOf:
              - name: r1
      )!!!";
      static const std::set<std::string> accepted({ "p1", "p2", "p3", "valuetype-set", "valuetype-set:vt1", "valuetype-set:vt2", "rule-set", "rule-set:r1" });
      static const std::set<std::string> used({ "valuetype-set", "valuetype-set:vt1", "valuetype-set:vt2", "rule-set", "rule-set:r1" });

      CurrentTest ct(*this, "testProperties");
      Owned<IDataMaskingProfileContext> context(createContext(cfgTxt, 0));
      assertAcceptedProperties(context, accepted);
      assertUsedProperties(context, used);
      assertContextProperties(context, accepted);
    }
    void testVersioning()
    {
      static const char* cfgTxt = R"!!!(
        profile:
          domain: 'urn:hpcc:unittest'
          minimumVersion: 10
          defaultVersion: 15
          maximumVersion: 20
          property:
          - name: this
            maximumVersion: 12
          - name: this
            minimumVersion: 15
          - name: that
          valueType:
          - name: foo
            minimumVersion: 12
            maximumVersion: 13
            memberOf:
            - name: this
            maskStyle:
            - name: custom-default
              maximumVersion: 12
              overrideDefault: true
              pattern: '#'
            - name: custom-default
              minimumVersion: 13
              overrideDefault: true
              pattern: '+'
            rule:
            - startToken: <Mask>
              endToken: </Mask>
              matchCase: true
              maximumVersion: 12
              name: sensitive
            - startToken: <Mask>
              endToken: </Mask>
              minimumVersion: 13
              name: insensitive
      )!!!";

      CurrentTest ct(*this, "testVersioning");
      Owned<Profile> profile(createProfile(cfgTxt, true));
      assertVersions(profile, 10, 15, 20);
      assertAcceptsProperty(profile, "this",               { 1, 1, 1, 0, 0, 1, 1, 1, 1, 1, 1 });
      assertAcceptsProperty(profile, "that",               { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 });
      assertUsesProperty(profile, "this",                  { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 });
      assertUsesProperty(profile, "that",                  { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 });
      assertAcceptsProperty(profile, "valuetype-set",      { 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0 });
      assertAcceptsProperty(profile, "valuetype-set:this", { 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0 });
      assertUsesProperty(profile, "valuetype-set",         { 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0 });
      assertUsesProperty(profile, "valuetype-set:this",    { 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0 });
      Owned<IDataMaskingProfileContext> context(profile->createContext(12, nullptr));
      if (!context->setProperty("valuetype-set", "this"))
      {
        fprintf(stdout, "\n%s: failed to set 'valuetype-set'\n", currentTest);
        CPPUNIT_ASSERT(false);
      }
      if (!context->hasProperty("valuetype-set"))
      {
        fprintf(stdout, "\n%s: failed to find 'valuetype-set'\n", currentTest);
        CPPUNIT_ASSERT(false);
      }
      if (isEmptyString(context->queryProperty("valuetype-set")))
      {
        fprintf(stdout, "\n%s: failed to query 'valuetype-set'\n", currentTest);
        CPPUNIT_ASSERT(false);
      }
      assertMaskContent(context, nullptr, "<mask>oops</mask>", nullptr);
      assertMaskContent(context, nullptr, "<Mask>mask</Mask>", "<Mask>####</Mask>");
      context.setown(profile->createContext(13, nullptr));
      if (!context->setProperty("valuetype-set", "this"))
      {
        fprintf(stdout, "\n%s: failed to set 'valuetype-set'\n", currentTest);
        CPPUNIT_ASSERT(false);
      }
      if (!context->hasProperty("valuetype-set"))
      {
        fprintf(stdout, "\n%s: failed to find 'valuetype-set'\n", currentTest);
        CPPUNIT_ASSERT(false);
      }
      if (isEmptyString(context->queryProperty("valuetype-set")))
      {
        fprintf(stdout, "\n%s: failed to query 'valuetype-set'\n", currentTest);
        CPPUNIT_ASSERT(false);
      }
      assertMaskContent(context, nullptr, "<mask>oops</mask>", "<mask>++++</mask>");
      assertMaskContent(context, nullptr, "<Mask>mask</Mask>", "<Mask>++++</Mask>");
    }
    void testUnconditional()
    {
      static const char* cfgTxt = R"!!!(
        profile:
          domain: 'urn:hpcc:unittest'
          valueType:
          - name: foo
          - name: '*'
            maskStyle:
              name: override
              overrideDefault: true
              pattern: +
      )!!!";

      CurrentTest ct(*this, "testUnconditional");
      Owned<IDataMaskingProfileContext> context(createContext(cfgTxt, 0));
      assertMaskValueConditionally(context, "foo", nullptr, "sample data", "***********");
      assertMaskValueConditionally(context, "bar", nullptr, "sample data", nullptr);
      assertMaskValueUnconditionally(context, "foo", nullptr, "sample data", "***********");
      assertMaskValueUnconditionally(context, "bar", nullptr, "sample data", "+++++++++++");
      assertMaskValueUnconditionally(context, "*", nullptr, "sample data", "+++++++++++");
    }
    void testTraceOperation()
    {
      static const char* cfgTxt = R"!!!(
    profile:
      domain: 'urn:hpcc:esdl:integration'
      valueType:
        name: password
        rule:
          startToken: <password>
          endToken: </password>
      )!!!";
      static const char* input = R"!!!(
<MaskContentRequest>
 <Username>me</Username>
 <Password>password</Password>
</MaskContentRequest>
      )!!!";
      static const char* expect = R"!!!(
<MaskContentRequest>
 <Username>me</Username>
 <Password>********</Password>
</MaskContentRequest>
      )!!!";

      CurrentTest ct(*this, "testTraceOperation");
      Owned<IDataMaskingProfileContext> context(createContext(cfgTxt, 0));
      assertMaskContent(context, "xml", input, expect);
    }

protected:
    Profile* createProfile(const char* cfgTxt, bool expectValid)
    {
      Owned<IPTree>  cfg;
      try
      {
        cfg.setown(createPTreeFromYAMLString(cfgTxt));
      }
      catch (IException *e)
      {
        StringBuffer msg;
        e->errorMessage(msg);
        fprintf(stdout, "\n%s: exception while parsing '%s' [%s]\n", currentTest, cfgTxt, msg.str());
        e->Release();
        throw SubstituteException();
      }
      IPTree* pRoot = cfg->queryBranch("profile");
      Owned<ITracer> tracer(createTracer());
      Owned<Profile> profile = new Profile(*tracer);
      bool configured = profile->configure(*pRoot);
      if (expectValid)
      {
        if (!configured)
        {
          fprintf(stdout, "\n%s: failed to configure profile\n", currentTest);
          CPPUNIT_ASSERT(false);
        }
        if (!profile->isValid())
        {
          fprintf(stdout, "\n%s: invalid profile\n", currentTest);
          CPPUNIT_ASSERT(false);
        }
      }
      return profile.getClear();
    }

    IDataMaskingProfileContext* createContext(const char* cfgTxt, uint8_t version)
    {
      Owned<Profile> profile(createProfile(cfgTxt, true));
      return profile->createContext(version, nullptr);
    }

    void assertDefaultDomain(Profile* profile, const char* expectedId)
    {
      const char* actualId = profile->queryDefaultDomain();
      if (expectedId && actualId)
      {
        if (!streq(expectedId, actualId))
        {
          fprintf(stdout, "\n%s: default domain mismatch ('%s' versus '%s')\n", currentTest, expectedId, actualId);
          CPPUNIT_ASSERT(false);
        }
      }
      else if (expectedId)
      {
        fprintf(stdout, "\n%s: expected default domain '%s'\n", currentTest, expectedId);
        CPPUNIT_ASSERT(false);
      }
      else if (actualId)
      {
        fprintf(stdout, "\n%s: unexpected default domain '%s'\n", currentTest, actualId);
        CPPUNIT_ASSERT(false);
      }
    }

    void assertAcceptedDomains(Profile* profile, const std::set<std::string>& expectedIds)
    {
      assertTextIterator(profile->getAcceptedDomains(), expectedIds, "domain ID");
    }

    void assertTextIterator(ITextIterator* it, const std::set<std::string>& expectedValues, const char* label)
    {
      bool failed = false;
      Owned<ITextIterator> rawValues(it);
      std::set<std::string> actualValues;
      ForEach(*it)
      {
        std::string value(it->query());
        if (!actualValues.insert(value).second)
        {
          fprintf(stdout, "\n%s: duplicate actual %s '%s'\n", currentTest, label, value.c_str());
          failed = true;
        }
        else if (expectedValues.find(value) == expectedValues.end())
        {
          fprintf(stdout, "\n%s: unexpected actual %s '%s'\n", currentTest, label, value.c_str());
          failed = true;
        }
      }
      if (!checkStringSets(actualValues, expectedValues, label))
        failed = true;
      CPPUNIT_ASSERT(!failed);
    }

    bool checkStringSets(const std::set<std::string>& actual, const std::set<std::string>& expected, const char* label)
    {
      bool failed = false;
      if (expected != actual)
      {
        for (const std::string& v : expected)
        {
          if (actual.find(v) != actual.end())
          {
            fprintf(stdout, "\n%s: missing expected %s '%s'\n", currentTest, label, v.c_str());
            failed = true;
          }
        }
      }
      return !failed;
    }

    void assertVersions(DataMasking::Versioned* versioned, uint8_t minVer, uint8_t defaultVer, uint8_t maxVer)
    {
        bool failed = false;
        if (minVer && versioned->queryMinimumVersion() != minVer)
        {
          fprintf(stdout, "\n%s: minimum version mismatch (%hhu <> %hhu)\n", currentTest, versioned->queryMinimumVersion(), minVer);
          failed = true;
        }
        if (defaultVer && versioned->queryDefaultVersion() != defaultVer)
        {
          fprintf(stdout, "\n%s: default version mismatch (%hhu <> %hhu)\n", currentTest, versioned->queryDefaultVersion(), defaultVer);
          failed = true;
        }
        if (maxVer && versioned->queryMaximumVersion() != maxVer)
        {
          fprintf(stdout, "\n%s: maximum version mismatch (%hhu <> %hhu)\n", currentTest, versioned->queryMaximumVersion(), maxVer);
          failed = true;
        }
        CPPUNIT_ASSERT(!failed);
    }

    void assertContexts(Profile* profile)
    {
      bool failed = false;
      uint8_t version = 0;
      Owned<IDataMaskingProfileContext> context(profile->createContext(version, nullptr));
      if (!checkContext(context, profile->queryDefaultVersion()))
        failed = true;
      for (version = 1; version < profile->queryMinimumVersion(); version++)
      {
        context.setown(profile->createContext(version, nullptr));
        if (!checkContext(context, 0))
          failed = true;
      }
      for (; version <= profile->queryMaximumVersion(); version++)
      {
        context.setown(profile->createContext(version, nullptr));
        if (!checkContext(context, version))
          failed = true;
      }
      for (; version != 0; version++)
      {
        context.setown(profile->createContext(version, nullptr));
        if (!checkContext(context, 0))
          failed = true;
      }
      CPPUNIT_ASSERT(!failed);
    }

    bool checkContext(IDataMaskingProfileContext* context, uint8_t expectedVersion)
    {
      bool failed = false;
      if (context)
      {
        uint8_t actualVersion = context->queryVersion();
        if (!expectedVersion)
        {
          fprintf(stdout, "\n%s: unexpected context version %hhu\n", currentTest, actualVersion);
          failed = true;
        }
        else if (context->queryVersion() != expectedVersion)
        {
          fprintf(stdout, "\n%s: context version mismatch (%hhu <> %hhu)\n", currentTest, actualVersion, expectedVersion);
          failed = true;
        }
      }
      else
      {
        if (expectedVersion)
        {
          fprintf(stdout, "\n%s: context creation failure for version %hhu\n", currentTest, expectedVersion);
          failed = true;
        }
      }
      return !failed;
    }

    void assertValueTypes(IDataMaskingProfileContext* context, const std::set<std::string>& expectedNames)
    {
      assertEntityIterator(context->inspector().getValueTypes(), expectedNames, "value type name");
    }

    template <typename iterator_t>
    void assertEntityIterator(iterator_t* it, const std::set<std::string>& expected, const char* label)
    {
      bool failed = false;
      Owned<iterator_t> rawValues(it);
      std::set<std::string> actual;
      ForEach(*it)
      {
        std::string value(it->query().queryName());
        if (!actual.insert(value).second)
        {
          fprintf(stdout, "\n%s: duplicate actual %s '%s'\n", currentTest, label, value.c_str());
          failed = true;
        }
        else if (expected.find(value) == expected.end())
        {
          fprintf(stdout, "\n%s: unexpected actual %s '%s'\n", currentTest, label, value.c_str());
          failed = true;
        }
      }
      if (!checkStringSets(actual, expected, "value type name"))
          failed = true;
      CPPUNIT_ASSERT(!failed);
    }

    void assertMaskStyles(IDataMaskingProfileContext* context, const char* valueType, const std::set<std::string>& expectedNames)
    {
      IDataMaskingProfileValueType* vt = context->inspector().queryValueType(valueType);
      if (vt)
        assertEntityIterator(vt->getMaskStyles(context), expectedNames, "mask style name");
      else
      {
        fprintf(stdout, "\n%s: unrecognized value type '%s'\n", currentTest, valueType);
        CPPUNIT_ASSERT(false);
      }
    }

    void assertAcceptedProperties(IDataMaskingProfileContext* context, const std::set<std::string>& expectedNames)
    {
      assertTextIterator(context->inspector().getAcceptedProperties(), expectedNames, "accepted property");
    }

    void assertUsedProperties(IDataMaskingProfileContext* context, const std::set<std::string>& expectedNames)
    {
      assertTextIterator(context->inspector().getUsedProperties(), expectedNames, "used property");
    }

    void assertRules(IDataMaskingProfileContext* context, const char* contentType, size_t expectedRuleCount)
    {
      bool failed = false;
      const IDataMaskerInspector& inspector = context->inspector();
      if (expectedRuleCount)
      {
        if (!inspector.hasRule(contentType))
        {
          fprintf(stdout, "\n%s: missing expected rule\n", currentTest);
          failed = true;
        }
        if (inspector.countOfRules(contentType) != expectedRuleCount)
        {
          fprintf(stdout, "\n%s: rule count mismatch (%zu <> %zu) for contentType '%s'\n", currentTest, inspector.countOfRules(contentType), expectedRuleCount, contentType);
          failed = true;
        }
      }
      else
      {
        if (inspector.hasRule(contentType))
        {
          fprintf(stdout, "\n%s: unexpected rule\n", currentTest);
          failed = true;
        }
        if (inspector.countOfRules(contentType))
        {
          fprintf(stdout, "\n%s: unexpected rule count %zu\n", currentTest, inspector.countOfRules(contentType));
          failed = true;
        }
      }
      CPPUNIT_ASSERT(!failed);
    }

    using OpLambda = std::function<bool(char*)>;

    void assertMaskValue(IDataMaskingProfileContext* context, const char* valueType, const char* maskStyle, const char* input, const char* expectedChanges)
    {
      VStringBuffer qualifiers("value type '%s' and mask style '%s'", valueType ? valueType : "", maskStyle ? maskStyle : "");
      OpLambda op = [&](char* buffer) { return context->maskValue(valueType, maskStyle, buffer, 0, size_t(-1), true); };
      return assertMaskOperation(op, input, expectedChanges, qualifiers);
    }

    void assertMaskValueConditionally(IDataMaskingProfileContext* context, const char* valueType, const char* maskStyle, const char* input, const char* expectedChanges)
    {
      VStringBuffer qualifiers("value type '%s' and mask style '%s'", valueType ? valueType : "", maskStyle ? maskStyle : "");
      OpLambda op = [&](char* buffer) { return context->maskValueConditionally(valueType, maskStyle, buffer, 0, size_t(-1)); };
      return assertMaskOperation(op, input, expectedChanges, qualifiers);
    }

    void assertMaskValueUnconditionally(IDataMaskingProfileContext* context, const char* valueType, const char* maskStyle, const char* input, const char* expectedChanges)
    {
      VStringBuffer qualifiers("value type '%s' and mask style '%s'", valueType ? valueType : "", maskStyle ? maskStyle : "");
      OpLambda op = [&](char* buffer) { return context->maskValueUnconditionally(valueType, maskStyle, buffer, 0, size_t(-1)); };
      return assertMaskOperation(op, input, expectedChanges, qualifiers);
    }

    void assertMaskContent(IDataMaskingProfileContext* context, const char* contentType, const char* input, const char* expectedChanges)
    {
      VStringBuffer qualifiers("content type '%s'", contentType ? contentType : "");
      OpLambda op = [&](char* buffer) { return context->maskContent(contentType, buffer, 0, size_t(-1)); };
      return assertMaskOperation(op, input, expectedChanges, qualifiers);
    }

    void assertMaskMarkupValue(IDataMaskingProfileContext* context, const char* element, const char* attribute, const char* input, IDataMaskingDependencyCallback& callback, const char* expectedChanges)
    {
      VStringBuffer qualifiers("element '%s' and attribute '%s'", element ? element : "", attribute ? attribute : "");
      OpLambda op = [&](char* buffer) { return context->maskMarkupValue(element, attribute, buffer, 0, size_t(-1), callback); };
      return assertMaskOperation(op, input, expectedChanges, qualifiers);
    }

    using MaskOp = std::function<bool(char*)>;
    void assertMaskOperation(MaskOp op, const char* input, const char* expected, const char* qualifiers)
    {
      bool failed = false;
      char* buffer = nullptr;
      try
      {
        StringBuffer strbuf(input);
        if (!checkMaskOperation(op, input, const_cast<char*>(strbuf.str()), expected, qualifiers))
        {
          fprintf(stdout, "%s: failure using StringBuffer\n", currentTest);
          failed = true;
        }
        char* buffer = (input ? strdup(input) : nullptr);
        if (!checkMaskOperation(op, input, buffer, expected, qualifiers))
        {
          fprintf(stdout, "%s: failure using strdup\n", currentTest);
          failed = true;
        }
      }
      catch (...)
      {
        free(buffer);
        throw;
      }
      free(buffer);
      CPPUNIT_ASSERT(!failed);
    }
    bool checkMaskOperation(MaskOp op, const char* input, char* buffer, const char* expected, const char* qualifiers)
    {
      bool failed = false;
      bool result = op(buffer);
      if (input && expected)
      {
        if (!result)
        {
          fprintf(stdout, "\n%s: no mask update reported for input '%s' using %s\n", currentTest, input, qualifiers);
          failed = true;
        }
        if (!streq(buffer, expected))
        {
          fprintf(stdout, "\n%s: masked value mismatch for input '%s' using %s ('%s' <> '%s')\n", currentTest, input, qualifiers, buffer, expected);
          failed = true;
        }
      }
      else if (input)
      {
        if (result)
        {
          fprintf(stdout, "\n%s: mask update reported for input: '%s' using %s\n", currentTest, input, qualifiers);
          failed = true;
        }
        if (!streq(buffer, input))
        {
          fprintf(stdout, "\n%s: masked value mismatch for input '%s' using %s ('%s' <> '%s')\n", currentTest, input, qualifiers, buffer, input);
          failed = true;
        }
      }
      else if (result)
      {
        fprintf(stdout, "\n%s: mask update reported for no input using %s\n", currentTest, qualifiers);
        failed = true;
      }
      return !failed;
    }

    void assertContextProperties(IDataMaskingProfileContext* context, const std::set<std::string>& accepted)
    {
      for (const std::string& a : accepted)
      {
        const char* name = a.c_str();
        if (!context->setProperty(name, "foo"))
        {
          fprintf(stdout, "\n%s: setProperty(\"%s\", \"foo\") failed\n", currentTest, name);
          CPPUNIT_ASSERT(false);
        }
        if (!context->hasProperty(name))
        {
          fprintf(stdout, "\n%s: hasProperty(\"%s\") failed\n", currentTest, name);
          CPPUNIT_ASSERT(false);
        }
        const char* value = context->queryProperty(name);
        if (!value || !streq(value, "foo"))
        {
          fprintf(stdout, "\n%s: queryProperty(\"%s\") failed\n", currentTest, name);
          CPPUNIT_ASSERT(false);
        }
      }
      if (context->hasProperties() != !accepted.empty())
      {
        fprintf(stdout, "\n%s: hasProperties() mismatch\n", currentTest);
        CPPUNIT_ASSERT(false);
      }
      Owned<IDataMaskingContextPropertyIterator> actual(context->inspector().getProperties());
      std::set<std::string> sanityCheck;
      ForEach(*actual)
      {
        const IDataMaskingContextProperty& prop = actual->query();
        const char* name = prop.queryName();
        if (!sanityCheck.insert(name).second)
        {
          fprintf(stdout, "\n%s: duplicate actual property '%s'\n", currentTest, name);
          CPPUNIT_ASSERT(false);
        }
        else if (accepted.find(name) == accepted.end())
        {
          fprintf(stdout, "\n%s: unexpected actual property '%s'\n", currentTest, name);
          CPPUNIT_ASSERT(false);
        }
      }
      for (const std::string& a : accepted)
      {
        const char* name = a.c_str();
        if (!context->removeProperty(name))
        {
          fprintf(stdout, "\n%s: removeProperty(\"%s\") failed\n", currentTest, name);
          CPPUNIT_ASSERT(false);
        }
      }
    }

    void assertAcceptsProperty(Profile* profile, const char* name, const std::vector<bool>& expected)
    {
      bool failed = false;
      uint8_t minVersion = profile->queryMinimumVersion();
      uint8_t maxVersion = profile->queryMaximumVersion();
      if (maxVersion - minVersion + 1 != expected.size())
      {
        fprintf(stdout, "\n%s: invalid test parameters\n", currentTest);
        CPPUNIT_ASSERT(false);
      }
      for (uint8_t version = minVersion; version <= maxVersion; version++)
      {
        Owned<IDataMaskingProfileContext> context(profile->createContext(version, nullptr));
        if (expected[version - minVersion] != context->inspector().acceptsProperty(name))
        {
          fprintf(stdout, "\n%s: property acceptance mismatch for '%s' and version %hhu\n", currentTest, name, version);
          failed = true;
        }
      }
      CPPUNIT_ASSERT(!failed);
    }

    void assertUsesProperty(Profile* profile, const char* name, const std::vector<bool>& expected)
    {
      bool failed = false;
      uint8_t minVersion = profile->queryMinimumVersion();
      uint8_t maxVersion = profile->queryMaximumVersion();
      if (maxVersion - minVersion + 1 != expected.size())
      {
        fprintf(stdout, "\n%s: invalid test parameters\n", currentTest);
        CPPUNIT_ASSERT(false);
      }
      for (uint8_t version = minVersion; version <= maxVersion; version++)
      {
        Owned<IDataMaskingProfileContext> context(profile->createContext(version, nullptr));
        if (expected[version - minVersion] != context->inspector().usesProperty(name))
        {
          fprintf(stdout, "\n%s: property usage mismatch for '%s' and version %hhu\n", currentTest, name, version);
          failed = true;
        }
      }
      CPPUNIT_ASSERT(!failed);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( DataMaskingProfileTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( DataMaskingProfileTests, "datamaskingprofiletests" );


#endif // _USE_CPPUNIT
