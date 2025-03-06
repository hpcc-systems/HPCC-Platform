/*##############################################################################

    Copyright (C) 2021 HPCC Systems®.

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
#include "unittests.hpp"
#include "fxpp/FragmentedXmlPullParser.hpp"
#include "fxpp/FragmentedXmlAssistant.hpp"
#include "xpp/xpputils.h"
#include "espcontext.hpp"
#include "xpathprocessor.hpp"
#include "esdl_script.hpp"
#include "wsexcept.hpp"

#include <stdio.h>
#include "dllserver.hpp"
#include "thorplugin.hpp"
#include "eclrtl.hpp"
#include "rtlformat.hpp"

using namespace xpp;
using namespace fxpp;
using namespace fxpp::fxa;

static IPTree* getPTree(const char* subSection, const char* xml)
{
  Owned<IPTree> node;
  try
  {
    node.setown(createPTreeFromXMLString(xml));
  }
  catch (IException* e)
  {
    StringBuffer msg("Exception creating PTree");
    if (subSection)
      msg.appendf(" '%s' ", subSection);
    e->errorMessage(msg);
    e->Release();
    CPPUNIT_FAIL(msg.str());
  }
  return node.getClear();
}

static void compareXml(const char* actual, const char* expected)
{
  if (isEmptyString(expected))
    CPPUNIT_ASSERT_MESSAGE("Test expected empty text", isEmptyString(actual));
  else
  {
    CPPUNIT_ASSERT_MESSAGE("Test expected non-empty text", !isEmptyString(actual));
    Owned<IPTree> actualNode(getPTree("actual", actual));
    Owned<IPTree> expectedNode(getPTree("expected", expected));
    CPPUNIT_ASSERT_MESSAGE(VStringBuffer("actualNode:\n%s\n- expectedNode:\n%s", actual, expected), areMatchingPTrees(actualNode, expectedNode));
  }
}

class fxppParserTests : public CppUnit::TestFixture
{
    class MockLoader : public CInterfaceOf<IExternalContentLoader>
    {
    public:
      virtual bool loadAndInject(const IExternalDetection& detected, IExternalContentInjector& injector) const override
      {
        std::string key(detected.queryContentLocation());
        if (!isEmptyString(detected.queryContentLocationSubset()))
          key.append(".").append(detected.queryContentLocationSubset());
        auto it = m_resources.find(key);
        if (it != m_resources.end())
          return injector.injectContent(it->second, -1);
        return false;
      }
    private:
      using ResMap = std::map<std::string, const char*>;
      ResMap m_resources;
    public:
        MockLoader()
        {
          m_resources["not-well-formed"] = R"!!(<root><child></root>)!!";
          m_resources["self-reference"] = R"!!(<ut:ExternalReplace resource="self-reference" xmlns:ut="hpcc:test:esp:xpp"/>)!!";
          m_resources["foo"] = R"!!(<ut:Foo xmlns:ut="hpcc:test:esp:xpp"/>)!!";
          m_resources["no-namespace"] = R"!!(<ut:InheritedNS/>)!!";
          m_resources["nested-1"] = R"!!(<ut:Nested node="2"><![CDATA[<ut:Nested node="3" resource="nested-2"/>]]></ut:Nested>)!!";
          m_resources["nested-2"] = R"!!(<ut:Nested node="4"><![CDATA[<ut:Nested node="5">&lt;ut:Nested resource="nested-3" node="6"/&gt;</ut:Nested>]]></ut:Nested>)!!";
          m_resources["nested-3"] = R"!!(<ut:Nested node="7"/>)!!";
          m_resources["foo.bar"] = R"!!(<Bar/>)!!";
        }
    };

    struct TestConfig
    {
      bool expectSuccess = true;
      bool supportNamespaces = true;
      bool requestNamespaces = false;
      bool useAssistant = true;
      bool useExternalDetector = true;
      bool useExternalLoader = true;
      bool registerEmbeddedFragments = true;
      bool registerExternalFragments = true;
      const char* name;
      const char* nsUri;
      const char* primaryDetectionKey;
      const char* secondaryDetectionKey;

      TestConfig(const char* _name)
        : name(_name)
        , nsUri("hpcc:test:esp:xpp")
        , primaryDetectionKey("resource")
        , secondaryDetectionKey("section")
      {
      }
    };

    CPPUNIT_TEST_SUITE( fxppParserTests );
        CPPUNIT_TEST(testNoInput);
        CPPUNIT_TEST(testNullInput);
        CPPUNIT_TEST(testEmptyInput);
        CPPUNIT_TEST(testNotWellFormedInput);
        CPPUNIT_TEST(testExternalFragmentSansDetector);
        CPPUNIT_TEST(testExternalFragmentSansLoader);
        CPPUNIT_TEST(testExternalFragmentDetectionSansLoad);
        CPPUNIT_TEST(testNotWellFormedExternalFragment);
        CPPUNIT_TEST(testCircularExternalFragments);
        CPPUNIT_TEST(testNoEmbeddedNSPropagation);
        CPPUNIT_TEST(testNoExternalNSPropagation);
        CPPUNIT_TEST(testNoAssistant);
        CPPUNIT_TEST(testEmbeddedFragmentReplacement);
        CPPUNIT_TEST(testExternalFragmentReplaceSelfClosing);
        CPPUNIT_TEST(testExternalFragmentReplaceEmpty);
        CPPUNIT_TEST(testExternalFragmentReplaceWithChild);
        CPPUNIT_TEST(testExternalFragmentReplaceWithText);
        CPPUNIT_TEST(testExternalFragmentRetainSelfClosingParent);
        CPPUNIT_TEST(testExternalFragmentRetainEmptyParent);
        CPPUNIT_TEST(testExternalFragmentRetainWithChild);
        CPPUNIT_TEST(testExternalFragmentRetainWithText);
        CPPUNIT_TEST(testEmbeddedNSPropagation);
        CPPUNIT_TEST(testExternalNSPropagation);
        CPPUNIT_TEST(testUnrootedInput);
        CPPUNIT_TEST(testUnrootedEmbedded);
        CPPUNIT_TEST(testNoNSSupport);
        CPPUNIT_TEST(testNestedFragments);
        CPPUNIT_TEST(testSecondaryKey);
        CPPUNIT_TEST(testSkipDocument);
        CPPUNIT_TEST(testSkipRoot);
        CPPUNIT_TEST(testSkipChild);
        CPPUNIT_TEST(testSkipMalformed);
        CPPUNIT_TEST(testRequestNamespaces);
    CPPUNIT_TEST_SUITE_END();

public:
    fxppParserTests(){}

    void testRequestNamespaces()
    {
      checkRequestNamespace("ns", "hpcc:test:esp:xpp:prefix");
      checkRequestNamespace(nullptr, "hpcc:test:esp:xpp:default");
      // tja testing below
      // checkRequestNamespace("nsXXX", "hpcc:test:esp:xpp:prefix");
      // checkRequestNamespace(nullptr, "hpcc:test:esp:xpp:default");
    }

    void checkRequestNamespace(const char* prefix, const char* uri)
    {
      StringBuffer test("request-namespace");
      StringBuffer tag("UnitTest");
      StringBuffer nsAttr("xmlns");
      StringBuffer attr("foo");
      if (!isEmptyString(prefix))
      {
        test.appendf("-%s", prefix);
        tag.insert(0, ":");
        tag.insert(0, prefix);
        nsAttr.appendf(":%s", prefix);
        attr.insert(0, ":");
        attr.insert(0, prefix);
      }
      TestConfig cfg(test);
      cfg.useAssistant = false;
      cfg.requestNamespaces = true;
      VStringBuffer input("<%s %s=\"%s\" %s=\"bar\"></%s>", tag.str(), nsAttr.str(), uri, attr.str(), tag.str());
      StringBuffer output;

      std::unique_ptr<IFragmentedXmlPullParser> parser(createParser(cfg));
      StartTag stag;
      EndTag etag;
      int type;

      parser->setInput(input, input.length());
      while ((type = parser->next()) != IXmlPullParser::END_DOCUMENT)
      {
        switch (type)
        {
        case IXmlPullParser::START_TAG:
          parser->readStartTag(stag);
          output.appendf("<%s", stag.getQName());
          for (int i = 0, n = stag.getLength(); i < n; i++)
            output.appendf(" %s=\"%s\"", stag.getRawName(i), stag.getValue(i));
          output.append(">");
          CPPUNIT_ASSERT_EQUAL_MESSAGE("Local start tag mismatch", std::string("UnitTest"), std::string(stag.getLocalName()));
          CPPUNIT_ASSERT_EQUAL_MESSAGE("Start tag uri mismatch", std::string(uri), std::string(stag.getUri()));
          CPPUNIT_ASSERT_EQUAL_MESSAGE("Attribute count mismatch", 2, stag.getLength());

          if (!isEmptyString(prefix))
            CPPUNIT_ASSERT_EQUAL_MESSAGE("Local attribute name mismatch", std::string(prefix), std::string(stag.getLocalName(0)));
          else
            CPPUNIT_ASSERT_EQUAL_MESSAGE("Local attribute name mismatch", std::string("xmlns"), std::string(stag.getLocalName(0)));

          CPPUNIT_ASSERT_EQUAL_MESSAGE("Namespace attribute uri mismatch", std::string(uri), std::string(stag.getURI(0)));
          CPPUNIT_ASSERT_EQUAL_MESSAGE("Local attribute name mismatch", std::string("foo"), std::string(stag.getLocalName(1)));
          break;
        case IXmlPullParser::END_TAG:
          parser->readEndTag(etag);
          output.appendf("</%s>", etag.getQName());
          CPPUNIT_ASSERT_EQUAL_MESSAGE("Local end tag mismatch", std::string("UnitTest"), std::string(etag.getLocalName()));
          CPPUNIT_ASSERT_EQUAL_MESSAGE("End tag uri mismatch", std::string(uri), std::string(etag.getUri()));
          break;
        }
      }
      CPPUNIT_ASSERT_EQUAL_MESSAGE("Input not equal to output", std::string(input), std::string(output));
    }

    void testSkipDocument()
    {
      std::unique_ptr<IFragmentedXmlPullParser> parser(fxpp::createParser());
      const char* input = R"!!!(<Root><Child/></Root>)!!!";
      parser->setInput(input, int(strlen(input)));
      parser->skipSubTree();
      CPPUNIT_ASSERT_MESSAGE("Parser not left at expected position", parser->next() == XmlPullParser::END_DOCUMENT);
    }

    void testSkipRoot()
    {
      std::unique_ptr<IFragmentedXmlPullParser> parser(fxpp::createParser());
      const char* input = R"!!!(<Root><Child/></Root>)!!!";
      parser->setInput(input, int(strlen(input)));
      parser->next();
      parser->skipSubTree();
      CPPUNIT_ASSERT_MESSAGE("Parser not left at expected position", parser->next() == XmlPullParser::END_DOCUMENT);
    }

    void testSkipMalformed()
    {
      std::unique_ptr<IFragmentedXmlPullParser> parser(fxpp::createParser());
      const char* input = R"!!!(<Root>)!!!";
      parser->setInput(input, int(strlen(input)));
      CPPUNIT_ASSERT_THROW(parser->skipSubTree(), XmlPullParserException);
    }

    void testSkipChild()
    {
      std::unique_ptr<IFragmentedXmlPullParser> parser(fxpp::createParser());
      const char* input = R"!!!(<Root><Child/></Root>)!!!";
      parser->setInput(input, int(strlen(input)));
      parser->next();
      parser->next();
      parser->skipSubTree();
      CPPUNIT_ASSERT_MESSAGE("Parser not left at expected position", parser->next() == XmlPullParser::END_TAG);
    }

    void testNoInput()
    {
      TestConfig cfg("no-input");
      cfg.expectSuccess = false;
      cfg.useAssistant = false;
      runTest(cfg);
    }
    void testNullInput()
    {
      TestConfig cfg("null-input");
      cfg.expectSuccess = false;
      cfg.useAssistant = false;
      runTest(cfg, nullptr, nullptr);
    }
    void testEmptyInput()
    {
      TestConfig cfg("empty-input");
      cfg.expectSuccess = false;
      cfg.useAssistant = false;
      runTest(cfg, "");
    }
    void testNotWellFormedInput()
    {
      TestConfig cfg("not-well-formed-input");
      cfg.expectSuccess = false;
      cfg.useAssistant = false;
      const char* input = R"!!(<?xml version="1.0" encoding="UTF-8"?>
          <ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
            <ut:Incomplete>
          </ut:UnitTest>)!!";
      runTest(cfg, input, input);
    }
    void testExternalFragmentSansDetector()
    {
      TestConfig cfg("external-fragment-sans-detector");
      cfg.expectSuccess = false;
      cfg.useExternalDetector = false;
      const char* input = R"!!(
          <ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
            <ut:ExternalReplace resource="foo"/>
          </ut:UnitTest>)!!";
      runTest(cfg, input);
    }
    void testExternalFragmentSansLoader()
    {
      TestConfig cfg("external-fragment-sans-loader");
      cfg.expectSuccess = false;
      cfg.useExternalLoader = false;
      const char* input = R"!!(
          <ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
            <ut:ExternalReplace resource="foo"/>
          </ut:UnitTest>)!!";
      runTest(cfg, input);
    }
    void testExternalFragmentDetectionSansLoad()
    {
      TestConfig cfg("external-fragment-detection-sans-load");
      cfg.expectSuccess = false;
      const char* input = R"!!(
          <ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
            <ut:ExternalReplace resource="unknown"/>
          </ut:UnitTest>)!!";
      runTest(cfg, input);
    }
    void testNotWellFormedExternalFragment()
    {
      TestConfig cfg("not-well-formed-external-fragment");
      cfg.expectSuccess = false;
      const char* input = R"!!(
          <ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
            <ut:ExternalReplace resource="not-well-formed"/>
          </ut:UnitTest>)!!";
      runTest(cfg, input);
    }
    void testCircularExternalFragments()
    {
      TestConfig cfg("circular-external-fragments");
      cfg.expectSuccess = false;
      const char* input = R"!!(
          <ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
            <ut:ExternalReplace resource="self-reference"/>
          </ut:UnitTest>)!!";
      runTest(cfg, input);
    }
    void testNoEmbeddedNSPropagation()
    {
      TestConfig cfg("no-embedded-ns-propagation");
      cfg.expectSuccess = false;
      const char* input = R"!!(
          <ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
            <ut:EmbeddedReplace>
              <![CDATA[<ut:InheritNS/>]]>
            </ut:EmbeddedReplace>
          </ut:UnitTest>)!!";
      runTest(cfg, input, input);
    }
    void testNoExternalNSPropagation()
    {
      TestConfig cfg("no-external-ns-propagation");
      cfg.expectSuccess = false;
      const char* input = R"!!(
          <ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
            <ut:ExternalReplace resource="no-namespace"/>
          </ut:UnitTest>)!!";
      runTest(cfg, input, input);
    }

    void testNoAssistant()
    {
      TestConfig cfg("no-assistant");
      cfg.useAssistant = false;
      const char* input = R"!!(<?xml version="1.0" encoding="UTF-8"?>
      <ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
        <ut:EmbeddedReplace>
          <![CDATA[<ut:Unregistered/>]]>
        </ut:EmbeddedReplace>
      </ut:UnitTest>
      )!!";
      runTest(cfg, input, input);
    }
    void testEmbeddedFragmentReplacement()
    {
      TestConfig cfg("embedded-fragment-replacement");
      const char* input = R"!!(
          <ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
            <ut:EmbeddedReplace this="that">
              <![CDATA[<ut:Unregistered xmlns:ut="hpcc:test:esp:xpp"/>]]>
            </ut:EmbeddedReplace>
          </ut:UnitTest>)!!";
      const char* equivalent = R"!!(
          <ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
            <ut:Unregistered xmlns:ut="hpcc:test:esp:xpp"/>
          </ut:UnitTest>)!!";
      runTest(cfg, input, equivalent);
    }
    void testExternalFragmentReplaceSelfClosing()
    {
      TestConfig cfg("external-replace-self-closing");
      const char* input = R"!!(
          <ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
            <ut:ExternalReplace resource="foo"/>
          </ut:UnitTest>)!!";
      const char* equivalent = R"!!(
          <ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
            <ut:Foo/>
          </ut:UnitTest>)!!";
      runTest(cfg, input, equivalent);
    }
    void testExternalFragmentReplaceEmpty()
    {
      TestConfig cfg("external-replace-empty");
      const char* input = R"!!(
          <ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
            <ut:ExternalReplace resource="foo">
            </ut:ExternalReplace>
          </ut:UnitTest>)!!";
      const char* equivalent = R"!!(
          <ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
            <ut:Foo/>
          </ut:UnitTest>)!!";
      runTest(cfg, input, equivalent);
    }
    void testExternalFragmentReplaceWithChild()
    {
      TestConfig cfg("external-replace-with-child");
      cfg.expectSuccess = false;
      const char* input = R"!!(
          <ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
            <ut:ExternalReplace resource="foo">
              <ut:NotAllowed/>
            </ut:ExternalReplace>
          </ut:UnitTest>)!!";
      runTest(cfg, input);
    }
    void testExternalFragmentReplaceWithText()
    {
      TestConfig cfg("external-replace-with-text");
      cfg.expectSuccess = false;
      const char* input = R"!!(
          <ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
            <ut:ExternalReplace resource="foo">
              NotAllowed
            </ut:ExternalReplace>
          </ut:UnitTest>)!!";
      runTest(cfg, input);
    }
    void testExternalFragmentRetainSelfClosingParent()
    {
      TestConfig cfg("external-retain-self-closing-parent");
      const char* input = R"!!(
          <ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
            <ut:ExternalRetainParent resource="foo"/>
          </ut:UnitTest>)!!";
      const char* equivalent = R"!!(
          <ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
            <ut:ExternalRetainParent resource="foo"><ut:Foo/></ut:ExternalRetainParent>
          </ut:UnitTest>)!!";
      runTest(cfg, input, equivalent);
    }
    void testExternalFragmentRetainEmptyParent()
    {
      TestConfig cfg("external-retain-empty-parent");
      const char* input = R"!!(
          <ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
            <ut:ExternalRetainParent resource="foo"></ut:ExternalRetainParent>
          </ut:UnitTest>)!!";
      const char* equivalent = R"!!(
          <ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
            <ut:ExternalRetainParent resource="foo"><ut:Foo/></ut:ExternalRetainParent>
          </ut:UnitTest>)!!";
      runTest(cfg, input, equivalent);
    }
    void testExternalFragmentRetainWithChild()
    {
      TestConfig cfg("external-retain-with-child");
      cfg.expectSuccess = false;
      const char* input = R"!!(
          <ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
            <ut:ExternalRetainParent resource="foo"><ut:NotAllowed/></ut:ExternalRetainParent>
          </ut:UnitTest>)!!";
      runTest(cfg, input);
    }
    void testExternalFragmentRetainWithText()
    {
      TestConfig cfg("external-retain-with-text");
      cfg.expectSuccess = false;
      const char* input = R"!!(
          <ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
            <ut:ExternalRetainParent resource="foo">NotAllowed</ut:ExternalRetainParent>
          </ut:UnitTest>)!!";
      runTest(cfg, input);
    }
    void testEmbeddedNSPropagation()
    {
      TestConfig cfg("embedded-namespace-propagation");
      const char* input = R"!!(
          <ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
            <ut:ShareNS>
              <![CDATA[<ut:InheritNS/>]]>
            </ut:ShareNS>
          </ut:UnitTest>)!!";
      const char* equivalent = R"!!(
          <ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
            <ut:InheritNS/>
          </ut:UnitTest>)!!";
      runTest(cfg, input, equivalent);
    }
    void testExternalNSPropagation()
    {
      TestConfig cfg("external-namespace-propagation");
      const char* input = R"!!(
          <ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
            <ut:ShareNS resource="no-namespace"/>
          </ut:UnitTest>)!!";
      const char* equivalent = R"!!(
          <ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
            <ut:InheritedNS/>
          </ut:UnitTest>)!!";
      runTest(cfg, input, equivalent);
    }
    void testUnrootedInput()
    {
      TestConfig cfg("unrooted-input");
      const char* input = R"!!(<Node1/><Node2/>)!!";
      runTest(cfg, input);
    }
    void testUnrootedEmbedded()
    {
      TestConfig cfg("unrooted-embedded");
      const char* input = R"!!(<ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
            <ut:EmbeddedReplace>
              <![CDATA[<Node1/><Node2/>]]>
            </ut:EmbeddedReplace>
          </ut:UnitTest>)!!";
      const char* equivalent = R"!!(<ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
              <Node1/><Node2/>
          </ut:UnitTest>)!!";
      runTest(cfg, input, equivalent);
    }
    void testNoNSSupport()
    {
      TestConfig cfg("no-namespace-support");
      cfg.supportNamespaces = false;
      const char* input = R"!!(<ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
            <ut:EmbeddedReplace>
              <![CDATA[<Node1/><Node2/>]]>
            </ut:EmbeddedReplace>
          </ut:UnitTest>)!!";
      const char* equivalent = R"!!(<ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
            <Node1/><Node2/>
          </ut:UnitTest>)!!";
      runTest(cfg, input, equivalent);
    }
    void testNestedFragments()
    {
      TestConfig cfg("nested-fragments");
      const char* input = R"!!(
        <ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
          <ut:Nested node="1" resource="nested-1"/>
        </ut:UnitTest>
      )!!";
      const char* equivalent = R"!!(
        <ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
          <ut:Nested node="1" resource="nested-1">
            <ut:Nested node="2">
              <ut:Nested node="3" resource="nested-2">
                <ut:Nested node="4">
                  <ut:Nested node="5">
                    <ut:Nested node="6" resource="nested-3">
                      <ut:Nested node="7"/>
                    </ut:Nested>
                  </ut:Nested>
                </ut:Nested>
              </ut:Nested>
            </ut:Nested>
          </ut:Nested>
        </ut:UnitTest>
      )!!";
      runTest(cfg, input, equivalent);
    }
    void testSecondaryKey()
    {
      TestConfig cfg("secondary-key");
      const char* input = R"!!(<ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
            <ut:ExternalReplace resource="foo" match="bar"/>
          </ut:UnitTest>)!!";
      const char* equivalent = R"!!(<ut:UnitTest xmlns:ut="hpcc:test:esp:xpp">
            <Bar/>
          </ut:UnitTest>)!!";
      runTest(cfg, input, equivalent);
    }

    IFragmentedXmlPullParser* createParser(const TestConfig& cfg)
    {
      Owned<fxa::IAssistant> assistant;
      if (cfg.useAssistant)
      {
        assistant.setown(fxa::createAssistant());
        if (cfg.useExternalDetector)
          assistant->setExternalDetector(fxa::createExternalDetector());
        if (cfg.useExternalLoader)
          assistant->setExternalLoader(new MockLoader());
        if (cfg.registerEmbeddedFragments)
        {
          const char* nsUri = cfg.supportNamespaces ? cfg.nsUri : "";
          assistant->registerEmbeddedFragment(nsUri, "EmbeddedReplace", 0);
          assistant->registerEmbeddedFragment(nsUri, "EmbeddedRetainSiblings", FXPP_RETAIN_FRAGMENT_PARENT);
          assistant->registerEmbeddedFragment(nsUri, "ShareNS", FXPP_PROPAGATE_NAMESPACES);
          assistant->registerEmbeddedFragment(nsUri, "Nested", FXPP_PROPAGATE_NAMESPACES | FXPP_RETAIN_FRAGMENT_PARENT);
          assistant->registerEmbeddedFragment("", "ut:EmbeddedReplace", 0); // support namespace off
        }
        if (cfg.registerExternalFragments)
        {
          const char* nsUri = cfg.supportNamespaces ? cfg.nsUri : "";
          assistant->registerExternalFragment(nsUri, "ExternalReplace", 0);
          assistant->registerExternalFragment(nsUri, "ExternalRetainParent", FXPP_RETAIN_FRAGMENT_PARENT);
          assistant->registerExternalFragment(nsUri, "ExternalRetainSiblings", FXPP_RETAIN_FRAGMENT_PARENT);
          assistant->registerExternalFragment(nsUri, "ShareNS", FXPP_PROPAGATE_NAMESPACES);
          assistant->registerExternalFragment(nsUri, "Nested", FXPP_PROPAGATE_NAMESPACES | FXPP_RETAIN_FRAGMENT_PARENT);
        }
      }
      IFragmentedXmlPullParser* parser = fxpp::createParser();
      parser->setSupportNamespaces(cfg.supportNamespaces);
      parser->setRequestNamespaces(cfg.requestNamespaces);
      parser->setAssistant(assistant.getClear());
      return parser;
    }

    void parse(IXmlPullParser& xpp, StringBuffer& output)
    {
      StartTag stag;
      switch (xpp.next())
      {
      case IXmlPullParser::START_TAG:
        xpp.readStartTag(stag);
        serialize(xpp, stag, output.clear());
        break;
      case IXmlPullParser::END_TAG:
        CPPUNIT_FAIL("Bad test input; unexpected end tag");
        break;
      case IXmlPullParser::CONTENT:
        if (!xpp.whitespaceContent())
          CPPUNIT_FAIL("Bad test input; unexpected content");
        else
          parse(xpp, output);
        break;
      case IXmlPullParser::END_DOCUMENT:
        CPPUNIT_FAIL("Bad test input; unexpected end document");
        break;
      default:
        CPPUNIT_FAIL("Bad test input; unexpected token type");
        break;
      }
    }

    void runTest(const TestConfig& cfg)
    {
      std::unique_ptr<IFragmentedXmlPullParser> parser(createParser(cfg));
      try
      {
        parser->next();
      }
      catch (XmlPullParserException& xe)
      {
        if (cfg.expectSuccess)
          throw;
      }
      catch (IException* e)
      {
        StringBuffer m;
        int code = e->errorCode();
        e->errorMessage(m);
        e->Release();
        CPPUNIT_FAIL(VStringBuffer("IException thrown: %d - %s", code, m.str()));
      }
    }

    void runTest(const TestConfig& cfg, const char* testInput)
    {
      std::unique_ptr<IFragmentedXmlPullParser> testParser(createParser(cfg));
      StringBuffer                              testOutput;
      try
      {
        testParser->setInput(testInput, -1);
        parse(*testParser, testOutput);
        CPPUNIT_ASSERT_MESSAGE("Test did not throw an expected exception.", cfg.expectSuccess);
      }
      catch (XmlPullParserException& xe)
      {
        if (cfg.expectSuccess)
          throw;
      }
      catch (IException* e)
      {
        StringBuffer m;
        int code = e->errorCode();
        e->errorMessage(m);
        e->Release();
        CPPUNIT_FAIL(VStringBuffer("IException thrown: %d - %s", code, m.str()));
      }
    }

    void runTest(const TestConfig& cfg, const char* testInput, const char* equivalentInput)
    {
      std::unique_ptr<IFragmentedXmlPullParser> testParser(createParser(cfg));
      StringBuffer                              testOutput;
      bool                                      compareOutput = false;
      try
      {
        testParser->setInput(testInput, -1);
        parse(*testParser, testOutput);
        CPPUNIT_ASSERT_MESSAGE("Test did not throw an expected exception.", cfg.expectSuccess);
        compareOutput = true;
      }
      catch (XmlPullParserException& xe)
      {
        if (cfg.expectSuccess)
          throw;
      }
      catch (IException* e)
      {
        StringBuffer m;
        int code = e->errorCode();
        e->errorMessage(m);
        e->Release();
        CPPUNIT_FAIL(VStringBuffer("IException thrown: %d - %s", code, m.str()));
      }

      if (compareOutput)
      {
        std::unique_ptr<IXmlPullParser> baseParser(new XmlPullParser());
        StringBuffer                    equivalentOutput;
        try
        {
          baseParser->setSupportNamespaces(cfg.supportNamespaces);
          baseParser->setRequestNamespaces(cfg.requestNamespaces);
          baseParser->setInput(equivalentInput, int(strlen(equivalentInput)));
          parse(*baseParser, equivalentOutput);
        }
        catch (IException* e)
        {
          StringBuffer m;
          int code = e->errorCode();
          e->errorMessage(m);
          e->Release();
          CPPUNIT_FAIL(VStringBuffer("IException thrown: %d - %s", code, m.str()));
        }

        compareXml(testOutput, equivalentOutput);
      }
    }

    void serialize(StartTag& stag, StringBuffer& buffer)
    {
      const char* tag = stag.getLocalName();
      if (tag)
      {
          buffer.appendf("<%s", tag);

          for (int idx=0; idx<stag.getLength(); idx++)
          {
              buffer.appendf(" %s=\"", stag.getRawName(idx));
              buffer.append(stag.getValue(idx));
              buffer.append('\"');
          }

          buffer.append(">");
      }
    }
    void serialize(IXmlPullParser &xpp, StartTag &stag, StringBuffer & buffer)
    {
        int level = 1; //assumed due to the way gotonextdataset works.
        int type = XmlPullParser::END_TAG;
        const char * content = "";
        const char *tag = NULL;
        EndTag etag;

        serialize(stag, buffer);
        do
        {
            type = xpp.next();
            switch(type)
            {
                case XmlPullParser::START_TAG:
                {
                    xpp.readStartTag(stag);
                    ++level;

                    serialize(stag, buffer);
                    break;
                }
                case XmlPullParser::END_TAG:
                    xpp.readEndTag(etag);
                    tag = etag.getLocalName();
                    if (tag)
                        buffer.appendf("</%s>", tag);
                    --level;
                break;
                case XmlPullParser::CONTENT:
                    content = xpp.readContent();
                    if (!isEmptyString(content) && !xpp.whitespaceContent())
                        encodeXML(content, buffer);
                    break;
                case XmlPullParser::END_DOCUMENT:
                    level=0;
                break;
            }
        }
        while (level > 0);
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION( fxppParserTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( fxppParserTests, "fxppParser" );

#endif // _USE_CPPUNIT
