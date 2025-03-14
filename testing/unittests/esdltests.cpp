/*##############################################################################

    Copyright (C) 2020 HPCC Systems®.

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
#include "espcontext.hpp"
#include "xpathprocessor.hpp"
#include "esdl_script.hpp"
#include "wsexcept.hpp"
#include "txsummary.hpp"
#include "SecureUser.hpp"
#include "datamaskingengine.hpp"

#include <stdio.h>
#include "dllserver.hpp"
#include "thorplugin.hpp"
#include "eclrtl.hpp"
#include "rtlformat.hpp"

#include "nlohmann/json.hpp"

// =============================================================== URI parser

static constexpr const char * soapRequest = R"!!(<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
<soap:Body>
<extra>
<EchoPersonInfo>
  <_TransactionId>1736623372_3126765312_1333296170</_TransactionId>
  <Context>
   <Row>
    <Common>
     <TransactionId>1736623372_3126765312_1333296170</TransactionId>
    </Common>
   </Row>
  </Context>
  <EchoPersonInfoRequest>
   <Row>
    <Name>
     <First>Joe</First>
     <Last>Doe</Last>
    </Name>
    <Addresses>
     <Address>
      <type>Home</type>
      <Line1>101 Main street</Line1>
      <Line2>Apt 202</Line2>
      <City>Hometown</City>
      <State>HI</State>
      <Zip>96703</Zip>
     </Address>
    </Addresses>
   </Row>
  </EchoPersonInfoRequest>
</EchoPersonInfo>
</extra>
</soap:Body>
</soap:Envelope>
)!!";

static constexpr const char * esdlScript = R"!!(<es:CustomRequestTransform xmlns:es="urn:hpcc:esdl:script" target="soap:Body/extra/{$query}/{$request}">
   <es:variable name="var1"  select="'script'"/>
   <es:variable name="var2"  select="$var1"/>
   <es:param name='testcase' select="'unknown'"/>
   <es:param name='FailStrict' select='false()'/>
   <es:param name='FailLevel1A' select='false()'/>
   <es:param name='FailLevel1B' select='false()'/>
   <es:param name='AssertLevel1C' select='false()'/>
   <es:param name='FailLevel2A' select='false()'/>
   <es:param name='FailLevel2B' select='false()'/>
   <es:param name='AssertLevel2C' select='false()'/>
   <es:param name="param1" select="'script'"/>
   <es:param name="param2" select="$param1"/>
   <es:set-value target="TestCase"  select="$testcase"/>
   <es:set-value target="Var2"  select="$var2"/>
   <es:set-value target="Param2"  select="$param2"/>
   <es:choose>
      <es:when test="not(es:validateFeaturesAccess('AllowSomething : Read, AllowAnother : Full'))">
         <es:fail code="401" message="concat('authorization failed for something or other (', $clientversion, ')')"/>
      </es:when>
      <es:when test="not(es:getFeatureSecAccessFlags('AllowSomething')=es:secureAccessFlags('Full'))">
         <es:fail code="401" message="concat('auth flag check failed for something (', $clientversion, ')')"/>
      </es:when>
      <es:when test="$FailStrict">
         <es:if test="$undeclared">
           <es:fail code="1" message="'not strict'"/>
          </es:if>
      </es:when>
      <es:when test="$FailLevel1A">
         <es:fail code="11" message="'FailLevel1A'"/>
      </es:when>
      <es:when test="$FailLevel1B">
        <es:if test="$FailLevel1B">
           <es:fail code="12" message="'FailLevel1B'"/>
        </es:if>
      </es:when>
      <es:when test="$AssertLevel1C">
         <es:assert test="not($AssertLevel1C)" code="13" message="'AssertLevel1C'"/>
      </es:when>
      <es:otherwise>
         <es:set-value target="InnerTestCase"  select="$testcase"/>
         <es:choose>
            <es:when test="$FailLevel2A">
               <es:fail code="21" message="'FailLevel2A'"/>
            </es:when>
            <es:when test="$FailLevel2B">
              <es:if test="$FailLevel2B">
                 <es:fail code="22" message="'FailLevel2B'"/>
              </es:if>
            </es:when>
            <es:when test="$AssertLevel2C">
               <es:assert test="not($AssertLevel2C)" code="23" message="'AssertLevel2C'"/>
            </es:when>
            <es:otherwise>
               <es:set-value target="test"  select="'auth success'"/>
               <es:set-value target="Row/Name/Last" select="'XXX'"/>
               <es:set-value target="Row/Name/Last" select="'POE'"/>
               <es:append-to-value target="Row/AppendTo"  select="'This'"/>
               <es:append-to-value target="Row/AppendTo"  select="'One'"/>
               <es:append-to-value target="Row/AppendTo"  select="'String'"/>
               <es:add-value target="Row/Name/Aliases/Alias"  select="'moe'"/>
               <es:add-value target="Row/Name/Aliases/Alias"  select="'poe'"/>
               <es:add-value target="Row/Name/Aliases/Alias"  select="'roe'"/>
            </es:otherwise>
         </es:choose>
      </es:otherwise>
   </es:choose>
</es:CustomRequestTransform>
)!!";

static constexpr const char * esdlScriptNoPrefix = R"!!(<CustomRequestTransform xmlns="urn:hpcc:esdl:script" target="soap:Body/extra/{$query}/{$request}">
   <variable name="var1"  select="'script'"/>
   <variable name="var2"  select="$var1"/>
   <param name='testcase' select="'unknown'"/>
   <param name='FailStrict' select='false()'/>
   <param name='FailLevel1A' select='false()'/>
   <param name='FailLevel1B' select='false()'/>
   <param name='AssertLevel1C' select='false()'/>
   <param name='FailLevel2A' select='false()'/>
   <param name='FailLevel2B' select='false()'/>
   <param name='AssertLevel2C' select='false()'/>
   <param name="param1" select="'script'"/>
   <param name="param2" select="$param1"/>
   <set-value target="TestCase"  select="$testcase"/>
   <set-value target="Var2"  select="$var2"/>
   <set-value target="Param2"  select="$param2"/>
   <choose>
      <when test="not(validateFeaturesAccess('AllowSomething : Read, AllowAnother : Full'))">
         <fail code="401" message="concat('authorization failed for something or other (', $clientversion, ')')"/>
      </when>
      <when test="not(getFeatureSecAccessFlags('AllowSomething')=secureAccessFlags('Full'))">
         <fail code="401" message="concat('auth flag check failed for something (', $clientversion, ')')"/>
      </when>
      <when test="$FailStrict">
         <if test="$undeclared">
           <fail code="1" message="'not strict'"/>
          </if>
      </when>
      <when test="$FailLevel1A">
         <fail code="11" message="'FailLevel1A'"/>
      </when>
      <when test="$FailLevel1B">
        <if test="$FailLevel1B">
           <fail code="12" message="'FailLevel1B'"/>
        </if>
      </when>
      <when test="$AssertLevel1C">
         <assert test="not($AssertLevel1C)" code="13" message="'AssertLevel1C'"/>
      </when>
      <otherwise>
         <set-value target="InnerTestCase"  select="$testcase"/>
         <choose>
            <when test="$FailLevel2A">
               <fail code="21" message="'FailLevel2A'"/>
            </when>
            <when test="$FailLevel2B">
              <if test="$FailLevel2B">
                 <fail code="22" message="'FailLevel2B'"/>
              </if>
            </when>
            <when test="$AssertLevel2C">
               <assert test="not($AssertLevel2C)" code="23" message="'AssertLevel2C'"/>
            </when>
            <otherwise>
               <set-value target="test"  select="'auth success'"/>
               <set-value target="Row/Name/Last" select="'XXX'"/>
               <set-value target="Row/Name/Last" select="'POE'"/>
               <append-to-value target="Row/AppendTo"  select="'This'"/>
               <append-to-value target="Row/AppendTo"  select="'One'"/>
               <append-to-value target="Row/AppendTo"  select="'String'"/>
               <add-value target="Row/Name/Aliases/Alias"  select="'moe'"/>
               <add-value target="Row/Name/Aliases/Alias"  select="'poe'"/>
               <add-value target="Row/Name/Aliases/Alias"  select="'roe'"/>
            </otherwise>
         </choose>
      </otherwise>
   </choose>
</CustomRequestTransform>
)!!";

static constexpr const char * esdlScriptSelectPath = R"!!(
<es:CustomRequestTransform xmlns:es="urn:hpcc:esdl:script" target="soap:Body/extra/{$query}/{$request}">
  <es:param name="selectPath" select="''"/>

  <es:set-value target="_OUTPUT_" select="$selectPath"/>
</es:CustomRequestTransform>
)!!";

static constexpr const char * esdlImplicitNamespaceSelectPath = R"!!(
<es:CustomRequestTransform xmlns:es="urn:hpcc:esdl:script" target="soap:Body/n:extra/n:{$query}/n:{$request}">
  <es:param name="selectPath" select="''"/>

  <es:set-value target="_OUTPUT_" select="$selectPath"/>
</es:CustomRequestTransform>
)!!";

static constexpr const char* selectPathResult = R"!!(<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <extra>
      <EchoPersonInfo>
        <Context>
          <Row>
            <Common>
              <TransactionId>1736623372_3126765312_1333296170</TransactionId>
            </Common>
          </Row>
        </Context>
        <_TransactionId>1736623372_3126765312_1333296170</_TransactionId>
        <EchoPersonInfoRequest>
          <Row>
            <Addresses>
              <Address>
                <type>Home</type>
                <Line2>Apt 202</Line2>
                <Line1>101 Main street</Line1>
                <City>Hometown</City>
                <Zip>96703</Zip>
                <State>HI</State>
              </Address>
            </Addresses>
            <Name>
              <Last>Doe</Last>
              <First>Joe</First>
            </Name>
          </Row>
          <_OUTPUT_>Joe</_OUTPUT_>
        </EchoPersonInfoRequest>
      </EchoPersonInfo>
    </extra>
  </soap:Body>
</soap:Envelope>)!!";

bool areEquivalentTestXMLStrings(const char *xml1, const char *xml2, bool exact=false)
{
    if (isEmptyString(xml1) || isEmptyString(xml2))
        return false;
    Owned<IPropertyTree> tree1 = createPTreeFromXMLString(xml1);
    Owned<IPropertyTree> tree2 = createPTreeFromXMLString(xml2);

    //areMatchingPTrees may not compare actual tag content
    if (exact)
    {
      StringBuffer s1;
      StringBuffer s2;
      toXML(tree1, s1);
      toXML(tree2, s2);
      return streq(s1, s2);
    }
    return areMatchingPTrees(tree1, tree2);
}

static const char *target_config = "<method queryname='EchoPersonInfo'/>";

class ESDLTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( ESDLTests );
        CPPUNIT_TEST(testEsdlTransformScript);
        CPPUNIT_TEST(testEsdlTransformScriptNoPrefix);
        CPPUNIT_TEST(testEsdlTransformForEach);
        CPPUNIT_TEST(testEsdlTransformVarScope);
        CPPUNIT_TEST(testEsdlTransformLegacy);
        CPPUNIT_TEST(testEsdlTransformIgnoreScriptErrors);
        CPPUNIT_TEST(testEsdlTransformTargetXpathErrors);
        CPPUNIT_TEST(testEsdlTransformFailStrict);
        CPPUNIT_TEST(testEsdlTransformScriptVarParam);
        CPPUNIT_TEST(testEsdlTransformFailLevel1A);
        CPPUNIT_TEST(testEsdlTransformFailLevel1B);
        CPPUNIT_TEST(testEsdlTransformFailLevel1C);
        CPPUNIT_TEST(testEsdlTransformFailLevel2A);
        CPPUNIT_TEST(testEsdlTransformFailLevel2B);
        CPPUNIT_TEST(testEsdlTransformFailLevel2C);
        CPPUNIT_TEST(testEsdlTransformAnyDescendentPath);
        CPPUNIT_TEST(testEsdlTransformAbsoluteSoapPath);
        CPPUNIT_TEST(testEsdlTransformRelativePath);
        CPPUNIT_TEST(testEsdlTransformSelectPath);
        CPPUNIT_TEST(testEsdlTransformImplicitPrefix);
        CPPUNIT_TEST(testEsdlTransformRequestNamespaces);
        CPPUNIT_TEST(testScriptContext);
        CPPUNIT_TEST(testTargetElement);
        CPPUNIT_TEST(testStringFunctions);
        CPPUNIT_TEST(testTxSummary);
        CPPUNIT_TEST(testParamEx);
        CPPUNIT_TEST(testMaskingIntegration);
      //The following require setup, uncomment for development testing for now:
      //CPPUNIT_TEST(testMysql);
      //CPPUNIT_TEST(testCallFunctions); //requires a particular roxie query
      //CPPUNIT_TEST(testHTTPPostXml); //requires a particular roxie query
      //CPPUNIT_TEST(testSynchronizeHTTPPostXml); //requires a particular roxie query
    CPPUNIT_TEST_SUITE_END();

public:
    ESDLTests(){}

    inline const char *queryTestName(IPropertyTree *cfg)
    {
        const char *testname = cfg->queryProp("Transform/Param[@name='testcase']/@value");
        if (!testname)
            return "unknown";
        return testname;
    }

    IEsdlScriptContext *createTestScriptContext(IEspContext *ctx, const char *xml, const char *config, IEsdlFunctionRegister *functionRegister=nullptr)
    {
        Owned<IEsdlScriptContext> scriptContext = createEsdlScriptContext(ctx, functionRegister, nullptr);
        scriptContext->setTestMode(true);

        scriptContext->setAttribute(ESDLScriptCtxSection_ESDLInfo, "service", "EsdlExample");
        scriptContext->setAttribute(ESDLScriptCtxSection_ESDLInfo, "method", "EchoPersonInfo");
        scriptContext->setAttribute(ESDLScriptCtxSection_ESDLInfo, "request_type", "EchoPersonInfoRequest");
        scriptContext->setAttribute(ESDLScriptCtxSection_ESDLInfo, "request", "EchoPersonInfoRequest");

        scriptContext->setContent(ESDLScriptCtxSection_BindingConfig, config);
        scriptContext->setContent(ESDLScriptCtxSection_TargetConfig, target_config);
        scriptContext->setContent(ESDLScriptCtxSection_ESDLRequest, xml);
        return scriptContext.getClear();
    }

    void runTransform(IEsdlScriptContext *scriptContext, const char *scriptXml, const char *srcSection, const char *tgtSection, const char *testname)
    {
        unsigned startTime = msTick();
        Owned<IEsdlCustomTransform> tf = createEsdlCustomTransform(scriptXml, nullptr);

        tf->processTransform(scriptContext, srcSection, tgtSection);
        DBGLOG("Test (%s) time ms(%u)", testname, msTick() - startTime);
    }
    void runTest(const char *testname, const char *scriptXml, const char *xml, const char *config, const char *result, int code)
    {
        try
        {
            //DBGLOG("starting %s:\n", testname);  //uncomment to help debug
            Owned<IEspContext> ctx = createEspContext(nullptr);
            ctx->setUser(new CSecureUser("not-real", ""));
            Owned<IEsdlScriptContext> scriptContext = createTestScriptContext(ctx, xml, config);
            scriptContext->setTraceToStdout(false);
            runTransform(scriptContext, scriptXml, ESDLScriptCtxSection_ESDLRequest, ESDLScriptCtxSection_FinalRequest, testname);

            CPPUNIT_ASSERT_EQUAL_MESSAGE(VStringBuffer("Expected exception %d not thrown", code), code, 0);

            StringBuffer output;
            scriptContext->toXML(output.clear(), ESDLScriptCtxSection_FinalRequest);

            if (result)
            {
              VStringBuffer comparison("Expected: %s\nActual: %s", result, output.str());
              CPPUNIT_ASSERT_MESSAGE(comparison.str(), areEquivalentTestXMLStrings(result, output.str()));
            }
        }
        catch (IException *E)
        {
          StringBuffer m;
          int actualErrorCode = E->errorCode();
          VStringBuffer message("Exception message: %s", E->errorMessage(m).str());
          E->Release();
          CPPUNIT_ASSERT_EQUAL_MESSAGE(message.str(), code, actualErrorCode);
        }
    }

    void testEsdlTransformSelectPath()
    {
      constexpr const char* config = R"!!(
        <config strictParams='true'>
          <Transform>
            <Param name='testcase' value="select-path"/>
            <Param name='selectPath' select="//First"/>
          </Transform>
        </config>
      )!!";

      runTest("select-path", esdlScriptSelectPath, soapRequest, config, selectPathResult, 0);
    }

    void testEsdlTransformAbsoluteSoapPath()
    {
      constexpr const char* config = R"!!(
        <config strictParams='true'>
          <Transform>
            <Param name='testcase' value="absolute-soap-path"/>
            <Param name='selectPath' select="/esdl_script_context/esdl_request/soap:Envelope/soap:Body/extra/EchoPersonInfo/EchoPersonInfoRequest/Row/Name/First"/>
          </Transform>
        </config>
      )!!";

      runTest("absolute-soap-path", esdlScriptSelectPath, soapRequest, config, selectPathResult, 0);
    }

    void testEsdlTransformRelativePath()
    {
      constexpr const char* config = R"!!(
        <config strictParams='true'>
          <Transform>
            <Param name='testcase' value="relative-path"/>
            <Param name='selectPath' select="soap:Body/extra/EchoPersonInfo/EchoPersonInfoRequest/Row/Name/First"/>
          </Transform>
        </config>
      )!!";

      runTest("relative-path", esdlScriptSelectPath, soapRequest, config, selectPathResult, 0);
    }

    void testEsdlTransformAnyDescendentPath()
    {
      constexpr const char* config = R"!!(
        <config strictParams='true'>
          <Transform>
            <Param name='testcase' value="any-descendent-path"/>
            <Param name='selectPath' select="//First"/>
          </Transform>
        </config>
      )!!";

      runTest("any-descendent-path", esdlScriptSelectPath, soapRequest, config, selectPathResult, 0);
    }

    void testEsdlTransformImplicitPrefix()
    {
      static constexpr const char * soapRequestImplicitPrefix = R"!!(<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns="http://webservices.example.com/WsFooBar">
<soap:Body>
<extra>
<EchoPersonInfo>
  <_TransactionId>1736623372_3126765312_1333296170</_TransactionId>
  <Context>
   <Row>
    <Common>
     <TransactionId>1736623372_3126765312_1333296170</TransactionId>
    </Common>
   </Row>
  </Context>
  <EchoPersonInfoRequest>
   <Row>
    <Name>
     <First>Joe</First>
     <Last>Doe</Last>
    </Name>
    <Addresses>
     <Address>
      <type>Home</type>
      <Line1>101 Main street</Line1>
      <Line2>Apt 202</Line2>
      <City>Hometown</City>
      <State>HI</State>
      <Zip>96703</Zip>
     </Address>
    </Addresses>
   </Row>
  </EchoPersonInfoRequest>
</EchoPersonInfo>
</extra>
</soap:Body>
</soap:Envelope>
)!!";
      constexpr const char* implicitPrefixResult = R"!!(<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns="http://webservices.example.com/WsFooBar">
  <soap:Body>
    <extra>
      <EchoPersonInfo>
        <Context>
          <Row>
            <Common>
              <TransactionId>1736623372_3126765312_1333296170</TransactionId>
            </Common>
          </Row>
        </Context>
        <_TransactionId>1736623372_3126765312_1333296170</_TransactionId>
        <EchoPersonInfoRequest>
          <Row>
            <Addresses>
              <Address>
                <type>Home</type>
                <Line2>Apt 202</Line2>
                <Line1>101 Main street</Line1>
                <City>Hometown</City>
                <Zip>96703</Zip>
                <State>HI</State>
              </Address>
            </Addresses>
            <Name>
              <Last>Doe</Last>
              <First>Joe</First>
            </Name>
          </Row>
          <_OUTPUT_>Joe</_OUTPUT_>
        </EchoPersonInfoRequest>
      </EchoPersonInfo>
    </extra>
  </soap:Body>
</soap:Envelope>)!!";

      constexpr const char* config = R"!!(
        <config strictParams='true'>
          <Transform>
            <Param name='testcase' value="implicit-prefix"/>
            <Param name='selectPath' select="soap:Body/n:extra/n:EchoPersonInfo/n:EchoPersonInfoRequest/n:Row/n:Name/n:First"/>
          </Transform>
        </config>
      )!!";

      constexpr const char* configNoPrefix = R"!!(
        <config strictParams='true'>
          <Transform>
            <Param name='testcase' value="implicit-prefix-not-used"/>
            <Param name='selectPath' select="soap:Body/extra/EchoPersonInfo/EchoPersonInfoRequest/Row/Name/First"/>
          </Transform>
        </config>
      )!!";

      // The previous version of this test was incorrect in asserting the implicit 'n' prefix was required.
      // "implicit-prefix" shows that it can be used.
      runTest("implicit-prefix", esdlImplicitNamespaceSelectPath, soapRequestImplicitPrefix, config, implicitPrefixResult, 0);

      // "implicit-prefix-not-used" shows that it is optional.
      runTest("implicit-prefix-not-used", esdlImplicitNamespaceSelectPath, soapRequestImplicitPrefix, configNoPrefix, implicitPrefixResult, 0);
    }

    void testEsdlTransformRequestNamespaces()
    {
      static constexpr const char * soapRequestNsInvalid = R"!!(<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns="invalid.uri.string">
<soap:Body>
<extra>
<EchoPersonInfo>
  <_TransactionId>1736623372_3126765312_1333296170</_TransactionId>
  <Context>
   <Row>
    <Common>
     <TransactionId>1736623372_3126765312_1333296170</TransactionId>
    </Common>
   </Row>
  </Context>
  <EchoPersonInfoRequest>
   <Row>
    <Name>
     <First>Joe</First>
     <Last>Doe</Last>
    </Name>
   </Row>
  </EchoPersonInfoRequest>
</EchoPersonInfo>
</extra>
</soap:Body>
</soap:Envelope>
)!!";

      static constexpr const char * soapRequestNsArbitrary1 = R"!!(<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns="arbitary:urn:text.here?really">
<soap:Body>
<extra>
<EchoPersonInfo>
  <_TransactionId>1736623372_3126765312_1333296170</_TransactionId>
  <Context>
   <Row>
    <Common>
     <TransactionId>1736623372_3126765312_1333296170</TransactionId>
    </Common>
   </Row>
  </Context>
  <EchoPersonInfoRequest>
   <Row>
    <Name>
     <First>Joe</First>
     <Last>Doe</Last>
    </Name>
   </Row>
  </EchoPersonInfoRequest>
</EchoPersonInfo>
</extra>
</soap:Body>
</soap:Envelope>
)!!";

      static constexpr const char * soapRequestNsArbitrary2 = R"!!(<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns="http:hullabaloo//nonsense:foo:bar#fragment">
<soap:Body>
<extra>
<EchoPersonInfo>
  <_TransactionId>1736623372_3126765312_1333296170</_TransactionId>
  <Context>
   <Row>
    <Common>
     <TransactionId>1736623372_3126765312_1333296170</TransactionId>
    </Common>
   </Row>
  </Context>
  <EchoPersonInfoRequest>
   <Row>
    <Name>
     <First>Joe</First>
     <Last>Doe</Last>
    </Name>
   </Row>
  </EchoPersonInfoRequest>
</EchoPersonInfo>
</extra>
</soap:Body>
</soap:Envelope>
)!!";

      constexpr const char* namespaceResult = R"!!(<soap:Envelope xmlns="http://webservices.example.com/WsFooBar" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
 <soap:Body>
  <extra>
   <EchoPersonInfo>
    <Context>
     <Row>
      <Common>
       <TransactionId>1736623372_3126765312_1333296170</TransactionId>
      </Common>
     </Row>
    </Context>
    <_TransactionId>1736623372_3126765312_1333296170</_TransactionId>
    <EchoPersonInfoRequest>
     <Row>
      <Name>
       <Last>Doe</Last>
       <First>Joe</First>
      </Name>
     </Row>
     <_OUTPUT_>Joe</_OUTPUT_>
    </EchoPersonInfoRequest>
   </EchoPersonInfo>
  </extra>
 </soap:Body>
</soap:Envelope>
)!!";

      constexpr const char* namespaceResultArbitrary1 = R"!!(<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns="arbitary:urn:text.here?really">
  <soap:Body>
    <extra>
      <EchoPersonInfo>
        <Context>
          <Row>
            <Common>
              <TransactionId>1736623372_3126765312_1333296170</TransactionId>
            </Common>
          </Row>
        </Context>
        <_TransactionId>1736623372_3126765312_1333296170</_TransactionId>
        <EchoPersonInfoRequest>
          <Row>
            <Name>
              <Last>Doe</Last>
              <First>Joe</First>
            </Name>
          </Row>
          <_OUTPUT_>Joe</_OUTPUT_>
        </EchoPersonInfoRequest>
      </EchoPersonInfo>
    </extra>
  </soap:Body>
</soap:Envelope>)!!";

      constexpr const char* namespaceResultArbitrary2 = R"!!(<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns="http:hullabaloo//nonsense:foo:bar#fragment">
  <soap:Body>
    <extra>
      <EchoPersonInfo>
        <Context>
          <Row>
            <Common>
              <TransactionId>1736623372_3126765312_1333296170</TransactionId>
            </Common>
          </Row>
        </Context>
        <_TransactionId>1736623372_3126765312_1333296170</_TransactionId>
        <EchoPersonInfoRequest>
          <Row>
            <Name>
              <Last>Doe</Last>
              <First>Joe</First>
            </Name>
          </Row>
          <_OUTPUT_>Joe</_OUTPUT_>
        </EchoPersonInfoRequest>
      </EchoPersonInfo>
    </extra>
  </soap:Body>
</soap:Envelope>)!!";

      constexpr const char* configInvalidURI = R"!!(
        <config strictParams='true'>
          <Transform>
            <Param name='testcase' value="invalid-uri"/>
            <Param name='selectPath' select="soap:Body/n:extra/n:EchoPersonInfo/n:EchoPersonInfoRequest/n:Row/n:Name/n:First"/>
          </Transform>
        </config>
      )!!";

      constexpr const char* configArbitraryURI1 = R"!!(
        <config strictParams='true'>
          <Transform>
            <Param name='testcase' value="arbitrary-uri-1"/>
            <Param name='selectPath' select="soap:Body/n:extra/n:EchoPersonInfo/n:EchoPersonInfoRequest/n:Row/n:Name/n:First"/>
          </Transform>
        </config>
      )!!";

      constexpr const char* configArbitraryURI2 = R"!!(
        <config strictParams='true'>
          <Transform>
            <Param name='testcase' value="arbitrary-uri-2"/>
            <Param name='selectPath' select="soap:Body/n:extra/n:EchoPersonInfo/n:EchoPersonInfoRequest/n:Row/n:Name/n:First"/>
          </Transform>
        </config>
      )!!";

      // An invalid namespace URI that is expected to throw an exception
      runTest("invalid-uri", esdlScriptSelectPath, soapRequestNsInvalid, configInvalidURI, namespaceResult, 5684);

      // Weird but valid URIs for namespaces
      runTest("arbitrary-uri-1", esdlImplicitNamespaceSelectPath, soapRequestNsArbitrary1, configArbitraryURI1, namespaceResultArbitrary1, 0);
      runTest("arbitrary-uri-2", esdlImplicitNamespaceSelectPath, soapRequestNsArbitrary2, configArbitraryURI2, namespaceResultArbitrary2, 0);

    }

    void testEsdlTransformScript()
    {
        constexpr const char *config = R"!!(<config strictParams='true'>
  <Transform>
    <Param name='testcase' value="operations"/>
  </Transform>
</config>)!!";

constexpr const char * result = R"!!(<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <extra>
      <EchoPersonInfo>
        <Context>
          <Row>
            <Common>
              <TransactionId>1736623372_3126765312_1333296170</TransactionId>
            </Common>
          </Row>
        </Context>
        <_TransactionId>1736623372_3126765312_1333296170</_TransactionId>
        <EchoPersonInfoRequest>
          <InnerTestCase>operations</InnerTestCase>
          <TestCase>operations</TestCase>
          <Var2>script</Var2>
          <Row>
            <Addresses>
              <Address>
                <type>Home</type>
                <Line2>Apt 202</Line2>
                <Line1>101 Main street</Line1>
                <City>Hometown</City>
                <Zip>96703</Zip>
                <State>HI</State>
              </Address>
            </Addresses>
            <Name>
              <Last>POE</Last>
              <Aliases>
                <Alias>moe</Alias>
                <Alias>poe</Alias>
                <Alias>roe</Alias>
              </Aliases>
              <First>Joe</First>
            </Name>
            <AppendTo>ThisOneString</AppendTo>
          </Row>
          <test>auth success</test>
          <Param2>script</Param2>
        </EchoPersonInfoRequest>
      </EchoPersonInfo>
    </extra>
  </soap:Body>
</soap:Envelope>)!!";

        runTest("operations", esdlScript, soapRequest, config, result, 0);
    }

    void testEsdlTransformScriptNoPrefix()
    {
        constexpr const char *config = R"!!(<config strictParams='true'>
  <Transform>
    <Param name='testcase' value="noprefix"/>
  </Transform>
</config>)!!";

constexpr const char * result = R"!!(<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <extra>
      <EchoPersonInfo>
        <Context>
          <Row>
            <Common>
              <TransactionId>1736623372_3126765312_1333296170</TransactionId>
            </Common>
          </Row>
        </Context>
        <_TransactionId>1736623372_3126765312_1333296170</_TransactionId>
        <EchoPersonInfoRequest>
          <TestCase>noprefix</TestCase>
          <InnerTestCase>noprefix</InnerTestCase>
          <Var2>script</Var2>
          <Row>
            <Addresses>
              <Address>
                <type>Home</type>
                <Line2>Apt 202</Line2>
                <Line1>101 Main street</Line1>
                <City>Hometown</City>
                <Zip>96703</Zip>
                <State>HI</State>
              </Address>
            </Addresses>
            <Name>
              <Last>POE</Last>
              <Aliases>
                <Alias>moe</Alias>
                <Alias>poe</Alias>
                <Alias>roe</Alias>
              </Aliases>
              <First>Joe</First>
            </Name>
            <AppendTo>ThisOneString</AppendTo>
          </Row>
          <Param2>script</Param2>
          <test>auth success</test>
        </EchoPersonInfoRequest>
      </EchoPersonInfo>
    </extra>
  </soap:Body>
</soap:Envelope>)!!";

        runTest("noprefix", esdlScriptNoPrefix, soapRequest, config, result, 0);
    }

    void testEsdlTransformFailStrict()
    {
        constexpr const char *config = R"!!(<config strictParams='true'>
  <Transform>
    <Param name='testcase' value="fail strict"/>
    <Param name='FailStrict' select='true()'/>
    <Param name='undeclared' value='inaccessible'/>
  </Transform>
</config>)!!";

        runTest("fail strict", esdlScript, soapRequest, config, nullptr, 5682);

        constexpr const char *config2 = R"!!(<config strictParams='false'>
  <Transform>
    <Param name='testcase' value="not strict"/>
    <Param name='FailStrict' select='true()'/>
    <Param name='undeclared' select='true()'/>
  </Transform>
</config>)!!";

        runTest("not strict", esdlScript, soapRequest, config2, nullptr, 1);
    }

    void testEsdlTransformScriptVarParam()
    {
        constexpr const char *config = R"!!(<config strictParams='true'>
  <Transform>
    <Param name='testcase' value="varparam"/>
    <Param name='FailLevel1A' select='false()'/>
    <Param name='FailLevel1B' select='false()'/>
    <Param name='AssertLevel1C' select='false()'/>
    <Param name='FailLevel2A' select='false()'/>
    <Param name='FailLevel2B' select='false()'/>
    <Param name='AssertLevel2C' select='false()'/>
    <Param name='param1' value='provided'/>
    <Param name='param2' select="concat('produced and ', $param1)"/>
    <Param name='var1' value='provided'/>
    <Param name='var2' select="concat('produced and ', $var1)"/>
  </Transform>
</config>)!!";

constexpr const char * result = R"!!(<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <extra>
      <EchoPersonInfo>
        <Context>
          <Row>
            <Common>
              <TransactionId>1736623372_3126765312_1333296170</TransactionId>
            </Common>
          </Row>
        </Context>
        <_TransactionId>1736623372_3126765312_1333296170</_TransactionId>
        <EchoPersonInfoRequest>
          <InnerTestCase>varparam</InnerTestCase>
          <TestCase>varparam</TestCase>
          <Var2>script</Var2>
          <Row>
            <Addresses>
              <Address>
                <type>Home</type>
                <Line2>Apt 202</Line2>
                <Line1>101 Main street</Line1>
                <City>Hometown</City>
                <Zip>96703</Zip>
                <State>HI</State>
              </Address>
            </Addresses>
            <Name>
              <Last>POE</Last>
              <Aliases>
                <Alias>moe</Alias>
                <Alias>poe</Alias>
                <Alias>roe</Alias>
              </Aliases>
              <First>Joe</First>
            </Name>
            <AppendTo>ThisOneString</AppendTo>
          </Row>
          <test>auth success</test>
          <Param2>produced and provided</Param2>
        </EchoPersonInfoRequest>
      </EchoPersonInfo>
    </extra>
  </soap:Body>
</soap:Envelope>)!!";

        runTest("fail strict", esdlScript, soapRequest, config, result, 0);
    }

    void testEsdlTransformForEach()
    {
        static constexpr const char * input = R"!!(<?xml version="1.0" encoding="UTF-8"?>
        <root xmlns:xx1="urn:x1" xmlns:xx2="urn:x2">
        <extra>
          <Friends>
            <Name>
             <First>Joe</First>
             <xx1:Alias>Moe</xx1:Alias>
             <xx1:Alias>Poe</xx1:Alias>
             <xx1:Alias>Doe</xx1:Alias>
            </Name>
            <Name>
             <First>Jane</First>
             <xx1:Alias>Jan</xx1:Alias>
             <xx1:Alias>Janie</xx1:Alias>
             <xx1:Alias>Janet</xx1:Alias>
            </Name>
          </Friends>
          <Relatives>
            <Name>
             <First>Jonathon</First>
             <xx1:Alias>John</xx1:Alias>
             <xx1:Alias>Jon</xx1:Alias>
             <xx1:Alias>Johnny</xx1:Alias>
             <xx1:Alias>Johnnie</xx1:Alias>
            </Name>
            <Name>
             <First>Jennifer</First>
             <xx1:Alias>Jen</xx1:Alias>
             <xx1:Alias>Jenny</xx1:Alias>
             <xx1:Alias>Jenna</xx1:Alias>
            </Name>
          </Relatives>
        </extra>
        </root>
        )!!";

        static constexpr const char * forEachScript = R"!!(<es:CustomRequestTransform xmlns:es="urn:hpcc:esdl:script" xmlns:x1="urn:x1"  xmlns:x2="urn:x2" target="extra">
           <es:param name="ForBuildListPath"/>
           <es:param name="ForIdPath"/>
           <es:param name="section"/>
           <es:param name="garbage"/>

           <es:trace test="false()" label="will not show" select="'this will not show up'"/>
           <es:trace test="true()" label="will show" select="'this will show up'"/>
           <es:trace label="string" select="'this is some text'"/>
           <es:trace label="boolean" select="true()"/>
           <es:trace label="number" select="90"/>
           <es:trace label="FOR nodeset" select="$ForBuildListPath"/>
           <es:for-each select="$ForBuildListPath">
               <es:variable name="q" select="str:decode-uri('%27')"/>
               <es:variable name="path" select="concat($section, '/Name[First=', $q, First, $q, ']/Aliases', $garbage)"/>
               <es:for-each select="x1:Alias">
                 <es:choose>
                   <es:when test="position()=1">
                     <es:append-to-value xpath_target="$path" select="."/>
                   </es:when>
                   <es:otherwise>
                     <es:append-to-value xpath_target="$path" select="','"/>
                     <es:append-to-value xpath_target="$path" select="."/>
                   </es:otherwise>
                 </es:choose>
               </es:for-each>
           </es:for-each>
           <es:for-each select="$ForIdPath">
             <es:add-value target="People/Ids/Id" select="."/>
           </es:for-each>
        </es:CustomRequestTransform>
        )!!";

        constexpr const char * resultFriends = R"!!(<root xmlns:xx1="urn:x1" xmlns:xx2="urn:x2">
  <extra>
    <Relatives>
      <Name>
        <First>Jonathon</First>
        <xx1:Alias>John</xx1:Alias>
        <xx1:Alias>Jon</xx1:Alias>
        <xx1:Alias>Johnny</xx1:Alias>
        <xx1:Alias>Johnnie</xx1:Alias>
      </Name>
      <Name>
        <First>Jennifer</First>
        <xx1:Alias>Jen</xx1:Alias>
        <xx1:Alias>Jenny</xx1:Alias>
        <xx1:Alias>Jenna</xx1:Alias>
      </Name>
    </Relatives>
    <People>
      <Ids>
        <Id>Joe</Id>
        <Id>Jane</Id>
      </Ids>
    </People>
    <Friends>
      <Name>
        <Aliases>Moe,Poe,Doe</Aliases>
        <First>Joe</First>
        <xx1:Alias>Moe</xx1:Alias>
        <xx1:Alias>Poe</xx1:Alias>
        <xx1:Alias>Doe</xx1:Alias>
      </Name>
      <Name>
        <Aliases>Jan,Janie,Janet</Aliases>
        <First>Jane</First>
        <xx1:Alias>Jan</xx1:Alias>
        <xx1:Alias>Janie</xx1:Alias>
        <xx1:Alias>Janet</xx1:Alias>
      </Name>
    </Friends>
  </extra>
</root>)!!";

        constexpr const char *configFriends = R"!!(<config strictParams="true">
  <Transform>
    <Param name='testcase' value="for each friend"/>
    <Param name='section' select="'Friends'"/>
    <Param name='garbage' select="''"/>
    <Param name='ForBuildListPath' select='/esdl_script_context/esdl_request/root/extra/Friends/Name'/>
    <Param name='ForIdPath' select='extra/Friends/Name/First'/>
  </Transform>
</config>)!!";

        runTest("for each friend", forEachScript, input, configFriends, resultFriends, 0);

        constexpr const char * resultRelatives = R"!!(<root xmlns:xx1="urn:x1" xmlns:xx2="urn:x2">
  <extra>
    <Relatives>
      <Name>
        <Aliases>John,Jon,Johnny,Johnnie</Aliases>
        <First>Jonathon</First>
        <xx1:Alias>John</xx1:Alias>
        <xx1:Alias>Jon</xx1:Alias>
        <xx1:Alias>Johnny</xx1:Alias>
        <xx1:Alias>Johnnie</xx1:Alias>
      </Name>
      <Name>
        <Aliases>Jen,Jenny,Jenna</Aliases>
        <First>Jennifer</First>
        <xx1:Alias>Jen</xx1:Alias>
        <xx1:Alias>Jenny</xx1:Alias>
        <xx1:Alias>Jenna</xx1:Alias>
      </Name>
    </Relatives>
    <People>
      <Ids>
        <Id>Jonathon</Id>
        <Id>Jennifer</Id>
      </Ids>
    </People>
    <Friends>
      <Name>
        <First>Joe</First>
        <xx1:Alias>Moe</xx1:Alias>
        <xx1:Alias>Poe</xx1:Alias>
        <xx1:Alias>Doe</xx1:Alias>
      </Name>
      <Name>
        <First>Jane</First>
        <xx1:Alias>Jan</xx1:Alias>
        <xx1:Alias>Janie</xx1:Alias>
        <xx1:Alias>Janet</xx1:Alias>
      </Name>
    </Friends>
  </extra>
</root>)!!";

        constexpr const char *configRelatives = R"!!(<config strictParams="true">
  <Transform>
    <Param name='testcase' value="for each relative"/>
    <Param name='section' select="'Relatives'"/>
    <Param name='garbage' select="''"/>
    <Param name='ForBuildListPath' select='extra/Relatives/Name'/>
    <Param name='ForIdPath' select='/esdl_script_context/esdl_request/root/extra/Relatives/Name/First'/> <!--absolute path may change, highly frowned upon-->
  </Transform>
</config>)!!";

        runTest("for each relative", forEachScript, input, configRelatives, resultRelatives, 0);

        constexpr const char *configGarbagePathError = R"!!(<config strictParams="true">
  <Transform>
    <Param name='testcase' value="for each garbage path error"/>
    <Param name='section' select="'Friends'"/>
    <Param name='garbage' select="'##'"/>
    <Param name='ForBuildListPath' select='extra/Friends/Name'/>
    <Param name='ForIdPath' select='extra/Friends/Name/First'/>
  </Transform>
</config>)!!";

        runTest("for each garbage path error", forEachScript, input, configGarbagePathError, nullptr, 5682);

        constexpr const char * resultNada = R"!!(<root xmlns:xx1="urn:x1" xmlns:xx2="urn:x2">
  <extra>
    <Relatives>
      <Name>
        <First>Jonathon</First>
        <xx1:Alias>John</xx1:Alias>
        <xx1:Alias>Jon</xx1:Alias>
        <xx1:Alias>Johnny</xx1:Alias>
        <xx1:Alias>Johnnie</xx1:Alias>
      </Name>
      <Name>
        <First>Jennifer</First>
        <xx1:Alias>Jen</xx1:Alias>
        <xx1:Alias>Jenny</xx1:Alias>
        <xx1:Alias>Jenna</xx1:Alias>
      </Name>
    </Relatives>
    <Friends>
      <Name>
        <First>Joe</First>
        <xx1:Alias>Moe</xx1:Alias>
        <xx1:Alias>Poe</xx1:Alias>
        <xx1:Alias>Doe</xx1:Alias>
      </Name>
      <Name>
        <First>Jane</First>
        <xx1:Alias>Jan</xx1:Alias>
        <xx1:Alias>Janie</xx1:Alias>
        <xx1:Alias>Janet</xx1:Alias>
      </Name>
    </Friends>
  </extra>
</root>)!!";

        constexpr const char *configNada = R"!!(<config strictParams="true">
  <Transform>
    <Param name='testcase' value="for each nada target"/>
    <Param name='section' select="'Friends'"/>
    <Param name='garbage' select="'/nada'"/>
    <Param name='ForBuildListPath' select='extra/Nada/Name'/>
    <Param name='ForIdPath' select='extra/Friends/Name/First/Nada'/>
  </Transform>
</config>)!!";

        runTest("for each nada target", forEachScript, input, configNada, resultNada, 0);
    }

    void testEsdlTransformVarScope()
    {
        static constexpr const char * input = R"!!(<?xml version="1.0" encoding="UTF-8"?>
        <root>
        <extra>
          <Friends>
            <Name>
             <First>Joe</First>
             <Alias>Moe</Alias>
             <Alias>Poe</Alias>
             <Alias>Doe</Alias>
            </Name>
            <Name>
             <First>Jane</First>
             <Alias>Jan</Alias>
             <Alias>Janie</Alias>
             <Alias>Janet</Alias>
            </Name>
          </Friends>
        </extra>
        </root>
        )!!";

        static constexpr const char * script = R"!!(<CustomRequestTransform xmlns="urn:hpcc:esdl:script" target="extra">
           <variable name="local" select="'root|'"/>
           <variable name="halfway" select="'root|'"/>
           <variable name="global" select="'root|'"/>
           <append-to-value target="trace-local" select="$local"/>
           <append-to-value target="trace-halfway" select="$halfway"/>
           <append-to-value target="trace-global" select="$global"/>
           <if test="true()">
             <variable name="local" select="'if|'"/>
             <append-to-value target="trace-local" select="$local"/>
             <append-to-value target="trace-halfway" select="$halfway"/>
             <append-to-value target="trace-global" select="$global"/>
           </if>
           <for-each select="extra/Friends/Name">
               <variable name="local" select="'for1|'"/>
               <variable name="q" select="str:decode-uri('%27')"/>
               <variable name="path" select="concat('Friends/Name[First=', $q, First, $q, ']/Aliases')"/>
               <append-to-value target="trace-local" select="$local"/>
               <append-to-value target="trace-halfway" select="$halfway"/>
               <append-to-value target="trace-global" select="$global"/>
             <for-each select="Alias">
                 <variable name="halfway" select="'for2|'"/>
                 <variable name="local" select="'for2|'"/>
                 <append-to-value target="trace-local" select="$local"/>
                 <append-to-value target="trace-halfway" select="$halfway"/>
                 <append-to-value target="trace-global" select="$global"/>
               <choose>
                   <when test="position()=1">
                     <variable name="local" select="'when|'"/>
                     <append-to-value xpath_target="$path" select="."/>
                     <append-to-value target="trace-local" select="$local"/>
                     <append-to-value target="trace-halfway" select="$halfway"/>
                     <append-to-value target="trace-global" select="$global"/>
                   </when>
                   <otherwise>
                     <variable name="local" select="'otherwise|'"/>
                     <append-to-value xpath_target="$path" select="','"/>
                     <append-to-value xpath_target="$path" select="."/>
                     <append-to-value target="trace-local" select="$local"/>
                     <append-to-value target="trace-halfway" select="$halfway"/>
                     <append-to-value target="trace-global" select="$global"/>
                   </otherwise>
                 </choose>
               </for-each>
           </for-each>
        </CustomRequestTransform>
        )!!";

        constexpr const char * result = R"!!(<root>
  <extra>
    <trace-global>root|root|root|root|root|root|root|root|root|root|root|root|root|root|root|root|</trace-global>
    <Friends>
      <Name>
        <Alias>Moe</Alias>
        <Alias>Poe</Alias>
        <Alias>Doe</Alias>
        <Aliases>Moe,Poe,Doe</Aliases>
        <First>Joe</First>
      </Name>
      <Name>
        <Alias>Jan</Alias>
        <Alias>Janie</Alias>
        <Alias>Janet</Alias>
        <Aliases>Jan,Janie,Janet</Aliases>
        <First>Jane</First>
      </Name>
    </Friends>
    <trace-halfway>for2|for2|for2|for2|for2|for2|root|for2|for2|for2|for2|for2|for2|root|root|root|</trace-halfway>
    <trace-local>for2|when|for2|otherwise|for2|otherwise|for1|for2|when|for2|otherwise|for2|otherwise|for1|if|root|</trace-local>
  </extra>
</root>)!!";

        constexpr const char *config = R"!!(<config strictParams="true">
  <Transform>
    <Param name='testcase' value="variable scope"/>
  </Transform>
</config>)!!";

        runTest("variable scope", script, input, config, result, 0);
    }

    void testEsdlTransformLegacy()
    {
        static constexpr const char * input = R"!!(<?xml version="1.0" encoding="UTF-8"?>
         <root>
            <Person>
               <Name>
                  <First>Joe</First>
               </Name>
             </Person>
          </root>
        )!!";

        static constexpr const char * script = R"!!(<es:CustomRequestTransform xmlns:es="urn:hpcc:esdl:script" target="Person">
            <es:SetValue target="ID" select="Person/Name/First"/>
            <es:AppendValue target="Append" select="Person/Name/First"/>
            <es:AppendValue target="Append" value="'++'"/>
        </es:CustomRequestTransform>
        )!!";

        constexpr const char *config = R"!!(<config><Transform><Param name='testcase' value="legacy"/></Transform></config>)!!";

        constexpr const char * result = R"!!(<root>
  <Person>
    <Append>Joe++</Append>
    <Name>
      <First>Joe</First>
    </Name>
    <ID>Joe</ID>
  </Person>
</root>)!!";

        runTest("legacy", script, input, config, result, 0);
    }
    void testEsdlTransformIgnoreScriptErrors()
    {
        static constexpr const char * input = R"!!(<?xml version="1.0" encoding="UTF-8"?>
         <root>
            <Person>
               <Name>
                  <First>Joe</First>
               </Name>
             </Person>
          </root>
        )!!";

        static constexpr const char * script = R"!!(<es:CustomRequestTransform xmlns:es="urn:hpcc:esdl:script" target="Person">
            <es:SetValue optional='true'/>
            <es:AppendValue select="Person/Name/First" optional='true'/>
            <es:AppendValue target="Append" optional='true'/>
        </es:CustomRequestTransform>
        )!!";

        constexpr const char *config = R"!!(<config><Transform><Param name='testcase' value="ignore script errors"/></Transform></config>)!!";

        constexpr const char * result = R"!!(<root>
  <Person>
    <Name>
      <First>Joe</First>
    </Name>
  </Person>
</root>)!!";

        runTest("ignore script errors", script, input, config, result, 0);
    }
    void testEsdlTransformTargetXpathErrors()
    {
        static constexpr const char * input = R"!!(<?xml version="1.0" encoding="UTF-8"?>
         <root>
            <Person>
               <Name>
                  <First>Joe</First>
               </Name>
             </Person>
          </root>
        )!!";

        static constexpr const char * script = R"!!(<es:CustomRequestTransform xmlns:es="urn:hpcc:esdl:script" target="Person">
            <es:SetValue xpath_target="concat('ID', '##')" select="/root/Person/Name/First"/>
            <es:AppendValue target="Append" select="Person/Name/First"/>
            <es:AppendValue target="Append" value="'++'"/>
        </es:CustomRequestTransform>
        )!!";

        constexpr const char *config = R"!!(<config><Transform><Param name='testcase' value="target xpath errors"/></Transform></config>)!!";

        runTest("target xpath errors", script, input, config, nullptr, 5682); //createPropBranch: cannot create path : ID##

        static constexpr const char * script2 = R"!!(<es:CustomRequestTransform xmlns:es="urn:hpcc:esdl:script" target="Person">
            <es:SetValue target="ID##" select="/root/Person/Name/First"/>
            <es:AppendValue target="Append" select="Person/Name/First"/>
            <es:AppendValue target="Append" value="'++'"/>
        </es:CustomRequestTransform>
        )!!";

        runTest("target xpath errors", script2, input, config, nullptr, 5682); //createPropBranch: cannot create path : ID##
}
    void testEsdlTransformFailLevel1A()
    {
        constexpr const char *config = R"!!(<config>
  <Transform>
    <Param name='testcase' value="fail Level1A"/>
    <Param name='FailLevel1A' select='true()'/>
    <Param name='FailLevel1B' select='true()'/>
    <Param name='AssertLevel1C' select='true()'/>
    <Param name='FailLevel2A' select='true()'/>
    <Param name='FailLevel2B' select='true()'/>
    <Param name='AssertLevel2C' select='true()'/>
  </Transform>
</config>)!!";

        runTest("fail Leve1A", esdlScript, soapRequest, config, nullptr, 11);
    }

    void testEsdlTransformFailLevel1B()
    {
        constexpr const char *config = R"!!(<config>
  <Transform>
    <Param name='testcase' value="fail Level1B"/>
    <Param name='FailLevel1A' select='false()'/>
    <Param name='FailLevel1B' select='true()'/>
    <Param name='AssertLevel1C' select='true()'/>
    <Param name='FailLevel2A' select='true()'/>
    <Param name='FailLevel2B' select='true()'/>
    <Param name='AssertLevel2C' select='true()'/>
  </Transform>
</config>)!!";

        runTest("fail Level1B", esdlScript, soapRequest, config, nullptr, 12);
    }

    void testEsdlTransformFailLevel1C()
    {
        constexpr const char *config = R"!!(<config>
  <Transform>
    <Param name='testcase' value="fail Level1C"/>
    <Param name='FailLevel1A' select='false()'/>
    <Param name='FailLevel1B' select='false()'/>
    <Param name='AssertLevel1C' select='true()'/>
    <Param name='FailLevel2A' select='true()'/>
    <Param name='FailLevel2B' select='true()'/>
    <Param name='AssertLevel2C' select='true()'/>
  </Transform>
</config>)!!";

        runTest("fail Level1C", esdlScript, soapRequest, config, nullptr, 13);
    }

    void testEsdlTransformFailLevel2A()
    {
        constexpr const char *config = R"!!(<config>
  <Transform>
    <Param name='testcase' value="fail Level2A"/>
    <Param name='FailLevel1A' select='false()'/>
    <Param name='FailLevel1B' select='false()'/>
    <Param name='AssertLevel1C' select='false()'/>
    <Param name='FailLevel2A' select='true()'/>
    <Param name='FailLevel2B' select='true()'/>
    <Param name='AssertLevel2C' select='true()'/>
  </Transform>
</config>)!!";

        runTest("fail Level2A", esdlScript, soapRequest, config, nullptr, 21);
    }

    void testEsdlTransformFailLevel2B()
    {
        constexpr const char *config = R"!!(<config>
  <Transform>
    <Param name='testcase' value="fail Level2B"/>
    <Param name='FailLevel1A' select='false()'/>
    <Param name='FailLevel1B' select='false()'/>
    <Param name='AssertLevel1C' select='false()'/>
    <Param name='FailLevel2A' select='false()'/>
    <Param name='FailLevel2B' select='true()'/>
    <Param name='AssertLevel2C' select='true()'/>
  </Transform>
</config>)!!";

        runTest("fail Level2B", esdlScript, soapRequest, config, nullptr, 22);
    }

    void testEsdlTransformFailLevel2C()
    {
        constexpr const char *config = R"!!(<config>
  <Transform>
    <Param name='testcase' value="fail Level2C"/>
    <Param name='FailLevel1A' select='false()'/>
    <Param name='FailLevel1B' select='false()'/>
    <Param name='AssertLevel1C' select='false()'/>
    <Param name='FailLevel2A' select='false()'/>
    <Param name='FailLevel2B' select='false()'/>
    <Param name='AssertLevel2C' select='true()'/>
  </Transform>
</config>)!!";

        runTest("fail Level2C", esdlScript, soapRequest, config, nullptr, 23);
    }

    void testScriptContext()
    {
        static constexpr const char * input = R"!!(<?xml version="1.0" encoding="UTF-8"?>
         <root>
            <Person>
               <FullName>
                  <First>Joe</First>
                  <ID>GI101</ID>
                  <ID>GI102</ID>
               </FullName>
            </Person>
          </root>
        )!!";

        static constexpr const char * script = R"!!(<es:CustomRequestTransform xmlns:es="urn:hpcc:esdl:script" target="Person">
            <es:param name="testpass"/>
            <es:if test="es:storedValueExists('myvalue')">
              <es:set-value xpath_target="concat('check-value1-pass-', $testpass)" select="concat('already set as of pass-', $testpass)"/>
              <es:set-value target="myvalue" select="es:getStoredStringValue('myvalue')"/>
              <es:remove-node target="Name/ID[1]"/> <!-- removing first one changes the index count so each is 1 -->
              <es:remove-node target="Name/ID[1]"/>
            </es:if>
            <es:if test="es:storedValueExists('myvalue2')">
              <es:set-value xpath_target="concat('check-value2-pass-', $testpass)" select="concat('already set in pass-', $testpass)"/>
              <es:set-value target="myvalue2" select="es:getStoredStringValue('myvalue2')"/>
            </es:if>
            <es:if test="es:logOptionExists('option1')">
              <es:set-value xpath_target="concat('check-logging-option1-pass-', $testpass)" select="concat('already set in pass-', $testpass)"/>
              <es:set-value target="logging-option1" select="es:getLogOption('option1')"/>
              <es:set-value target="logging-option2" select="es:getLogOption('option2')"/>
              <es:set-value target="profile" select="es:getLogProfile()"/>
            </es:if>
            <es:if test="not(es:storedValueExists('myvalue'))">
              <es:set-value xpath_target="concat('check-set-pass-', $testpass)" select="concat('not already set in pass-', $testpass)"/>
              <es:store-value xpath_name="'myvalue2'" select="'another stored value'"/>
              <es:store-value name="myvalue" select="'this is a stored value'"/>
              <es:set-log-option name="option1" select="'this is a logging option value'"/>
              <es:set-log-option xpath_name="'option2'" select="'this is an xpath named logging option value'"/>
              <es:set-log-profile select="'myprofile'"/>
              <es:rename-node target="FullName" new_name="Name"/>
            </es:if>
        </es:CustomRequestTransform>
        )!!";

        constexpr const char *config1 = R"!!(<config>
          <Transform>
            <Param name='testcase' value="script context 1"/>
            <Param name='testpass' value="1"/>
          </Transform>
        </config>)!!";

        constexpr const char *config2 = R"!!(<config>
          <Transform>
            <Param name='testcase' value="script context 2"/>
            <Param name='testpass' value="2"/>
          </Transform>
        </config>)!!";

        try
        {
            Owned<IEspContext> ctx = createEspContext(nullptr);
            Owned<IEsdlScriptContext> scriptContext = createTestScriptContext(ctx, input, config1);
            runTransform(scriptContext, script, ESDLScriptCtxSection_ESDLRequest, "FirstPass", "script context 1");

            scriptContext->setContent(ESDLScriptCtxSection_BindingConfig, config2);
            runTransform(scriptContext, script, "FirstPass", "SecondPass", "script context 2");

            constexpr const char * result = R"!!(<root>
  <Person>
    <Name>
      <First>Joe</First>
    </Name>
    <myvalue>this is a stored value</myvalue>
    <myvalue2>another stored value</myvalue2>
    <logging-option1>this is a logging option value</logging-option1>
    <check-set-pass-1>not already set in pass-1</check-set-pass-1>
    <check-value2-pass-2>already set in pass-2</check-value2-pass-2>
    <logging-option2>this is an xpath named logging option value</logging-option2>
    <profile>myprofile</profile>
    <check-logging-option1-pass-2>already set in pass-2</check-logging-option1-pass-2>
    <check-value1-pass-2>already set as of pass-2</check-value1-pass-2>
  </Person>
</root>)!!";

            StringBuffer output;
            scriptContext->toXML(output, "SecondPass");
            if (result)
            {
                VStringBuffer comparison("Expected: %s\nActual: %s", result, output.str());
                CPPUNIT_ASSERT_MESSAGE(comparison.str(), areEquivalentTestXMLStrings(result, output.str()));
            }
        }
        catch (IException *E)
        {
            StringBuffer m;
            VStringBuffer exceptionMessage("Exception: code=%d, message=%s", E->errorCode(), E->errorMessage(m).str());
            E->Release();
            CPPUNIT_FAIL(exceptionMessage);
        }
    }

    void testTargetElement()
    {
        static constexpr const char * input = R"!!(<?xml version="1.0" encoding="UTF-8"?>
         <root>
            <Person>
               <FullName>
                  <First>Joe</First>
                  <ID>GI101</ID>
                  <ID>GI102</ID>
               </FullName>
               <Friends>Jane,John,Jaap,Jessica</Friends>
            </Person>
          </root>
        )!!";

        static constexpr const char * script = R"!!(<es:CustomRequestTransform xmlns:es="urn:hpcc:esdl:script" target="Person">
            <es:variable name='value' select="'abc'"/>
            <es:if-source xpath="NotThere">
               <es:element name="NeverHere">
                  <es:copy-of select="."/>
               </es:element>
            </es:if-source>
            <es:if-source xpath="Person">
              <es:set-value target="@found" select="true()"/>
            </es:if-source>
            <es:source xpath="Person">
              <es:ensure-target xpath='How/Did/We/Get'>
                 <es:element name='Here'>
                    <es:variable name="tkns" select="es:tokenize('aaa,bbb;ccc+yyy', ',;+')"/>
                    <es:variable name="friends" select="es:tokenize(Friends, ',')"/>
                    <es:set-value target="@whoknows" select="$value"/>
                    <es:set-value target="IDontKnow" select="$value"/>
                    <es:element name="CopyTokens">
                      <es:copy-of select="$tkns"/>
                    </es:element>
                    <es:element name="CopyFriends">
                      <es:copy-of select="$friends"/>
                    </es:element>
                    <es:element name="CopyFields">
                      <es:for-each select="FullName/*">
                          <es:element name="CopyField">
                            <es:copy-of select="."/>
                          </es:element>
                      </es:for-each>
                    </es:element>
                    <es:element name="CopyFullName">
                       <es:copy-of select="FullName" new_name='FullerName'/>
                       <es:set-value target="FullerName/@valid" select="true()"/>
                    </es:element>
                </es:element>
              </es:ensure-target>
            </es:source>
            <es:if-target xpath='PartName'>
               <es:set-value target="NotSet" select="$value"/>
            </es:if-target>
            <es:if-target xpath='FullName'>
               <es:set-value target="IsSet" select="$value"/>
            </es:if-target>
            <es:target xpath='FullName'>
               <es:set-value target="DidntFail" select="$value"/>
            </es:target>
        </es:CustomRequestTransform>
        )!!";

        constexpr const char *config1 = R"!!(<config>
          <Transform>
            <Param name='testcase' value="new features"/>
          </Transform>
        </config>)!!";

            constexpr const char * result = R"!!(<root>
  <Person found="true">
    <FullName>
      <First>Joe</First>
      <ID>GI101</ID>
      <ID>GI102</ID>
      <IsSet>abc</IsSet>
      <DidntFail>abc</DidntFail>
    </FullName>
    <Friends>Jane,John,Jaap,Jessica</Friends>
    <How>
      <Did>
        <We>
          <Get>
            <Here whoknows="abc">
              <IDontKnow>abc</IDontKnow>
              <CopyTokens>
                <token>aaa</token>
                <token>bbb</token>
                <token>ccc</token>
                <token>yyy</token>
              </CopyTokens>
              <CopyFriends>
                <token>Jane</token>
                <token>John</token>
                <token>Jaap</token>
                <token>Jessica</token>
              </CopyFriends>
              <CopyFields>
                <CopyField>
                  <First>Joe</First>
                </CopyField>
                <CopyField>
                  <ID>GI101</ID>
                </CopyField>
                <CopyField>
                  <ID>GI102</ID>
                </CopyField>
              </CopyFields>
              <CopyFullName>
                <FullerName valid="true">
                  <First>Joe</First>
                  <ID>GI101</ID>
                  <ID>GI102</ID>
                </FullerName>
              </CopyFullName>
            </Here>
          </Get>
        </We>
      </Did>
    </How>
  </Person>
</root>)!!";


        try {

            Owned<IEspContext> ctx = createEspContext(nullptr);
            Owned<IEsdlScriptContext> scriptContext = createTestScriptContext(ctx, input, config1);
            runTransform(scriptContext, script, ESDLScriptCtxSection_ESDLRequest, "FirstPass", "target element 1");

            StringBuffer output;
            scriptContext->toXML(output, "FirstPass");
            if (result)
            {
                VStringBuffer comparison("Expected: %s\nActual: %s", result, output.str());
                CPPUNIT_ASSERT_MESSAGE(comparison.str(), areEquivalentTestXMLStrings(result, output.str()));
            }
        }
        catch (IException *E)
        {
            StringBuffer m;
            VStringBuffer exceptionMessage("Exception: code=%d, message=%s", E->errorCode(), E->errorMessage(m).str());
            E->Release();
            CPPUNIT_FAIL(exceptionMessage);
        }
    }

    void testStringFunctions()
    {
        static constexpr const char * input = R"!!(<?xml version="1.0" encoding="UTF-8"?>
          <root>
            <Request>
            </Request>
          </root>
        )!!";

        static constexpr const char * stringFunctionsScript = R"!!(<es:CustomRequestTransform xmlns:es="urn:hpcc:esdl:script" source="Request" target="Request">
          <es:set-value target="deprecated-encrypt-empty" select="deprecatedEncryptString('') = ''"/>
          <es:set-value target="deprecated-decrypt-empty" select="deprecatedDecryptString('') = ''"/>
          <es:set-value target="to-encrypt" select="'this is not secret'"/>
          <es:set-value target="deprecated-encrypt-value" select="deprecatedEncryptString(to-encrypt) != to-encrypt"/>
          <es:set-value target="deprecated-decrypt-value" select="deprecatedDecryptString(deprecatedEncryptString(to-encrypt)) = to-encrypt"/>
          <es:set-value target="encode-base64-empty" select="encodeBase64String('') = ''"/>
          <es:set-value target="decode-base64-empty" select="decodeBase64String('') = ''"/>
          <es:set-value target="to-encode" select="'sample text to Base64 encode and decode'"/>
          <es:set-value target="encode-base64-value" select="encodeBase64String(to-encode) != to-encode"/>
          <es:set-value target="decode-base64-value" select="decodeBase64String(encodeBase64String(to-encode)) = to-encode"/>
          <es:set-value target="compress-empty" select="compressString('') = 'AAEAAAAA'"/>
          <es:variable name="blob" select="toXmlString(.)"/>
          <es:set-value target="compress-value" select="compressString($blob) != $blob"/>
          <es:set-value target="decompress-value" select="decompressString(compressString($blob)) = $blob"/>
          <es:variable name="escaped" select="escapeXmlCharacters($blob)"/>
          <es:set-value target="escape-xml-value" select="$escaped != $blob"/>
          <es:set-value target="unescape-xml-value" select="unescapeXmlCharacters($escaped) = $blob"/>
        </es:CustomRequestTransform>
        )!!";

        try {
          Owned<IEspContext> ctx = createEspContext(nullptr);
          Owned<IEsdlScriptContext> scriptContext = createEsdlScriptContext(ctx, nullptr, nullptr);
          scriptContext->setTestMode(true);
          scriptContext->setTraceToStdout(false);
          scriptContext->setAttribute(ESDLScriptCtxSection_ESDLInfo, "service", "EsdlExample");
          scriptContext->setAttribute(ESDLScriptCtxSection_ESDLInfo, "method", "EchoPersonInfo");
          scriptContext->setAttribute(ESDLScriptCtxSection_ESDLInfo, "request_type", "EchoPersonInfoRequest");
          scriptContext->setAttribute(ESDLScriptCtxSection_ESDLInfo, "request", "EchoPersonInfoRequest");
          scriptContext->setContent("StringFunctions", input);

          runTransform(scriptContext, stringFunctionsScript, "StringFunctions", nullptr, "StringFunctions 1");
          CPPUNIT_ASSERT(scriptContext->getXPathBool("//deprecated-encrypt-empty"));;
          CPPUNIT_ASSERT(scriptContext->getXPathBool("//deprecated-decrypt-empty"));
          CPPUNIT_ASSERT(scriptContext->getXPathBool("//deprecated-encrypt-value"));
          CPPUNIT_ASSERT(scriptContext->getXPathBool("//deprecated-decrypt-value"));
          CPPUNIT_ASSERT(scriptContext->getXPathBool("//encode-base64-empty"));
          CPPUNIT_ASSERT(scriptContext->getXPathBool("//decode-base64-empty"));
          CPPUNIT_ASSERT(scriptContext->getXPathBool("//encode-base64-value"));
          CPPUNIT_ASSERT(scriptContext->getXPathBool("//decode-base64-value"));
          CPPUNIT_ASSERT(scriptContext->getXPathBool("//compress-empty"));
          CPPUNIT_ASSERT(scriptContext->getXPathBool("//compress-value"));
          CPPUNIT_ASSERT(scriptContext->getXPathBool("//decompress-value"));
          CPPUNIT_ASSERT(scriptContext->getXPathBool("//escape-xml-value"));
          CPPUNIT_ASSERT(scriptContext->getXPathBool("//unescape-xml-value"));
        }
        catch (IException *E)
        {
          StringBuffer m;
          VStringBuffer exceptionMessage("Exception: code=%d, message=%s", E->errorCode(), E->errorMessage(m).str());
          E->Release();
          CPPUNIT_FAIL(exceptionMessage);
        }
    }

    void testTxSummary()
    {
        static constexpr const char * input = R"!!(<?xml version="1.0" encoding="UTF-8"?>
          <root>
            <Request>
            </Request>
          </root>
        )!!";

        static constexpr const char * levelsScript = R"!!(<es:CustomRequestTransform xmlns:es="urn:hpcc:esdl:script" target="Request">
          <es:tx-summary-value name="level-default" select="''"/>
          <es:tx-summary-value name="level-min" select="''" level="min"/>
          <es:tx-summary-value name="level-normal" select="''" level="normal"/>
          <es:tx-summary-value name="level-max" select="''" level="max"/>
          <es:tx-summary-value name="level-1" select="''" level="1"/>
          <es:tx-summary-value name="level-2" select="''" level="2"/>
          <es:tx-summary-value name="level-3" select="''" level="3"/>
          <es:tx-summary-value name="level-4" select="''" level="4"/>
          <es:tx-summary-value name="level-5" select="''" level="5"/>
          <es:tx-summary-value name="level-6" select="''" level="6"/>
          <es:tx-summary-value name="level-7" select="''" level="7"/>
          <es:tx-summary-value name="level-8" select="''" level="8"/>
          <es:tx-summary-value name="level-9" select="''" level="9"/>
          <es:tx-summary-value name="level-10" select="''" level="10"/>
          <es:set-value target="level-default" select="getTxSummary('json')"/>
          <es:set-value target="level-min" select="getTxSummary('json', 'min')"/>
          <es:set-value target="level-normal" select="getTxSummary('json', 'normal')"/>
          <es:set-value target="level-max" select="getTxSummary('json', 'max')"/>
          <es:set-value target="level-1" select="getTxSummary('json', 1)"/>
          <es:set-value target="level-2" select="getTxSummary('json', 2)"/>
          <es:set-value target="level-3" select="getTxSummary('json', 3)"/>
          <es:set-value target="level-4" select="getTxSummary('json', 4)"/>
          <es:set-value target="level-5" select="getTxSummary('json', 5)"/>
          <es:set-value target="level-6" select="getTxSummary('json', 6)"/>
          <es:set-value target="level-7" select="getTxSummary('json', 7)"/>
          <es:set-value target="level-8" select="getTxSummary('json', 8)"/>
          <es:set-value target="level-9" select="getTxSummary('json', 9)"/>
          <es:set-value target="level-10" select="getTxSummary('json', 10)"/>
        </es:CustomRequestTransform>
        )!!";
        static const char* levelsResult1 = R"!!!({"level-default": "", "level-min": "", "level-1": ""})!!!";
        static const char* levelsResult2 = R"!!!({"level-default": "", "level-min": "", "level-1": "", "level-2": ""})!!!";
        static const char* levelsResult3 = R"!!!({"level-default": "", "level-min": "", "level-1": "", "level-2": "", "level-3": ""})!!!";
        static const char* levelsResult4 = R"!!!({"level-default": "", "level-min": "", "level-1": "", "level-2": "", "level-3": "", "level-4": ""})!!!";
        static const char* levelsResult5 = R"!!!({"level-default": "", "level-min": "", "level-normal": "", "level-1": "", "level-2": "", "level-3": "", "level-4": "", "level-5": ""})!!!";
        static const char* levelsResult6 = R"!!!({"level-default": "", "level-min": "", "level-normal": "", "level-1": "", "level-2": "", "level-3": "", "level-4": "", "level-5": "", "level-6": ""})!!!";
        static const char* levelsResult7 = R"!!!({"level-default": "", "level-min": "", "level-normal": "", "level-1": "", "level-2": "", "level-3": "", "level-4": "", "level-5": "", "level-6": "", "level-7": ""})!!!";
        static const char* levelsResult8 = R"!!!({"level-default": "", "level-min": "", "level-normal": "", "level-1": "", "level-2": "", "level-3": "", "level-4": "", "level-5": "", "level-6": "", "level-7": "", "level-8": ""})!!!";
        static const char* levelsResult9 = R"!!!({"level-default": "", "level-min": "", "level-normal": "", "level-1": "", "level-2": "", "level-3": "", "level-4": "", "level-5": "", "level-6": "", "level-7": "", "level-8": "", "level-9": ""})!!!";
        static const char* levelsResult10 = R"!!!({"level-default": "", "level-min": "", "level-normal": "", "level-max": "", "level-1": "", "level-2": "", "level-3": "", "level-4": "", "level-5": "", "level-6": "", "level-7": "", "level-8": "", "level-9": "", "level-10": ""})!!!";
        static constexpr const char * typesScript = R"!!(<es:CustomRequestTransform xmlns:es="urn:hpcc:esdl:script" target="Request">
          <es:tx-summary-value name="type-default" select="'0'"/>
          <es:tx-summary-value name="type-text" select="'0'" type="text"/>
          <es:tx-summary-value name="type-signed-min" select="'-9223372036854775808'" type="signed"/>
          <es:tx-summary-value name="type-signed-max" select="'9223372036854775807'" type="signed"/>
          <es:tx-summary-value name="type-unsigned-min" select="'0'" type="unsigned"/>
          <es:tx-summary-value name="type-unsigned-max" select="'18446744073709551615'" type="unsigned"/>
          <es:tx-summary-value name="type-decimal-min" select="'-1.79769e+308'" type="decimal"/>
          <es:tx-summary-value name="type-decimal-max" select="'1.79769e+308'" type="decimal"/>
          <es:set-value target="types-all" select="getTxSummary('json')"/>
        </es:CustomRequestTransform>
        )!!";
        static const char* typesResultAll = R"!!!({"type-default": "0", "type-text": "0", "type-signed-min": -9223372036854775808, "type-signed-max": 9223372036854775807, "type-unsigned-min": 0, "type-unsigned-max": 18446744073709551615, "type-decimal-min": -1.79769e+308, "type-decimal-max": 1.79769e+308})!!!";
        static constexpr const char * timersScript = R"!!(<es:CustomRequestTransform xmlns:es="urn:hpcc:esdl:script" target="Request">
          <es:tx-summary-timer name="scalar-default">
            <es:delay millis="2"/>
          </es:tx-summary-timer>
          <es:tx-summary-timer name="scalar-append" mode="append">
            <es:delay millis="2"/>
          </es:tx-summary-timer>
          <es:tx-summary-timer name="scalar-set" mode="set">
            <es:delay/>
          </es:tx-summary-timer>
          <es:tx-summary-timer name="accumulating" mode="accumulate">
            <es:delay millis="5"/>
          </es:tx-summary-timer>
          <es:tx-summary-timer name="accumulating" mode="accumulate">
            <es:delay millis="10"/>
          </es:tx-summary-timer>
          <es:set-value target="timers-all" select="getTxSummary('json')"/>
        </es:CustomRequestTransform>
        )!!";
        static const char* timersResultAll = R"!!!({"scalar-default": 2, "scalar-append": 2, "scalar-set": 1, "accumulating": 15})!!!";

        try {
          Owned<IEspContext> ctx = createEspContext(nullptr);
          CTxSummary* txSummary = ctx->queryTxSummary();
          Owned<IEsdlScriptContext> scriptContext = createEsdlScriptContext(ctx, nullptr, nullptr);
          scriptContext->setTestMode(true);
          scriptContext->setTraceToStdout(false);
          scriptContext->setAttribute(ESDLScriptCtxSection_ESDLInfo, "service", "EsdlExample");
          scriptContext->setAttribute(ESDLScriptCtxSection_ESDLInfo, "method", "EchoPersonInfo");
          scriptContext->setAttribute(ESDLScriptCtxSection_ESDLInfo, "request_type", "EchoPersonInfoRequest");
          scriptContext->setAttribute(ESDLScriptCtxSection_ESDLInfo, "request", "EchoPersonInfoRequest");
          scriptContext->setContent(ESDLScriptCtxSection_ESDLRequest, input);

          txSummary->clear();
          runTransform(scriptContext, levelsScript, ESDLScriptCtxSection_ESDLRequest, "TxSummary", "TxSummary 1");
          compareTxSummary(scriptContext, "level", "default", levelsResult1, true);
          compareTxSummary(scriptContext, "level", "min", levelsResult1, true);
          compareTxSummary(scriptContext, "level", "normal", levelsResult5, true);
          compareTxSummary(scriptContext, "level", "max", levelsResult10, true);
          compareTxSummary(scriptContext, "level", "1", levelsResult1, true);
          compareTxSummary(scriptContext, "level", "2", levelsResult2, true);
          compareTxSummary(scriptContext, "level", "3", levelsResult3, true);
          compareTxSummary(scriptContext, "level", "4", levelsResult4, true);
          compareTxSummary(scriptContext, "level", "5", levelsResult5, true);
          compareTxSummary(scriptContext, "level", "6", levelsResult6, true);
          compareTxSummary(scriptContext, "level", "7", levelsResult7, true);
          compareTxSummary(scriptContext, "level", "8", levelsResult8, true);
          compareTxSummary(scriptContext, "level", "9", levelsResult9, true);
          compareTxSummary(scriptContext, "level", "10", levelsResult10, true);

          txSummary->clear();
          runTransform(scriptContext, typesScript, ESDLScriptCtxSection_ESDLRequest, "TxSummary", "TxSummary 2");
          compareTxSummary(scriptContext, "types", "all", typesResultAll, true);

          txSummary->clear();
          runTransform(scriptContext, timersScript, ESDLScriptCtxSection_ESDLRequest, "TxSummary", "TxSummary 3");
          compareTxSummary(scriptContext, "timers", "all", timersResultAll, false);
        }
        catch (IException *E)
        {
          StringBuffer m;
          VStringBuffer exceptionMessage("Exception: code=%d, message=%s", E->errorCode(), E->errorMessage(m).str());
          E->Release();
          CPPUNIT_FAIL(exceptionMessage);
        }
    }
    void compareTxSummary(IEsdlScriptContext* scriptContext, const char* prefix, const char* test, const char* expectedText, bool exact)
    {
      using namespace nlohmann;
      StringBuffer actualText;
      scriptContext->getXPathString(VStringBuffer("TxSummary/root/Request/%s-%s", prefix, test), actualText);
      json expected = json::parse(expectedText);
      json actual = json::parse(actualText.str());
      if (expected != actual)
      {
        VStringBuffer comparison("Test(TxSummary-%s-%s) expected: '%s'\n    actual: '%s'", prefix, test, expectedText, actualText.str());
        // Timer values are not guaranteed. The delay operation ensures a minimum elapsed time
        // but cannot guarantee a maximum. While object equality is preferred, ensuring that at
        // least the expected times passed are is necessarily sufficient.
        bool closeEnough = !exact && expected.size() == actual.size();
        for (json::iterator it = expected.begin(); closeEnough && it != expected.end(); ++it)
        {
          CPPUNIT_ASSERT_MESSAGE(comparison, actual.contains(it.key()));
          CPPUNIT_ASSERT_MESSAGE(comparison, actual[it.key()] >= it.value());
        }
      }
    }

    void testParamEx()
    {
      static constexpr const char* input = R"!!!(
        <root>
          <Request/>
        </root>
      )!!!";
      static constexpr const char* script = R"!!!(
        <es:CustomRequestTransform xmlns:es="urn:hpcc:esdl:script" target="Request">
          <es:param name="espUserStatusString" select="'defaulted'"/>
          <es:assert test="$espUserStatusString != 'defaulted'" code="'1001'" message="'did not find defined input'"/>
          <es:source xpath="Request">
            <es:param name="bar" select="'defaulted'"/>
            <es:assert test="$bar = 'defaulted'" code="'1002'" message="'did not assign explicit default value'"/>
          </es:source>
          <es:source xpath="Request">
            <es:param name="baz"/>
            <es:assert test="string-length($baz) = 0" code="'1003'" message="'did not assign implied default value'"/>
          </es:source>
          <es:source xpath="Request">
            <es:param name="bar" failure_code="'1004'" failure_message="'did not find required input'"/>
          </es:source>
        </es:CustomRequestTransform>
      )!!!";

      runTest("param extension", script, input, nullptr, nullptr, 1004);
    }

    struct HistoricalEvent
    {
      LogMsgAudience msgAudience;
      LogMsgClass    msgClass;
      StringBuffer   msg;
      HistoricalEvent(const LogMsgCategory& category, const char* format, va_list arguments) __attribute__((format(printf, 3, 0)))
      {
        msgAudience = category.queryAudience();
        msgClass = category.queryClass();
        msg.valist_appendf(format, arguments);
      }
      HistoricalEvent(const LogMsgCategory& category, const char* _msg)
      {
        msgAudience = category.queryAudience();
        msgClass = category.queryClass();
        msg.append(_msg);
      }
      bool operator == (const HistoricalEvent& right) const
      {
        if ((msgAudience == right.msgAudience) && (msgClass == right.msgClass))
        {
          if (streq(msg, right.msg))
            return true;
          try
          {
            return areEquivalentTestXMLStrings(msg, right.msg, true);
          }
          catch (IException* e)
          {
            e->Release();
          }
          catch (...)
          {
          }
        }
        return false;
      }
      bool operator != (const HistoricalEvent& right) const
      {
        return !(*this == right);
      }
    };
    using History = std::list<HistoricalEvent>;
    class CMockTraceMsgSink : public CInterfaceOf<IModularTraceMsgSink>
    {
    public:
      virtual void valog(const LogMsgCategory& category, const char* format, va_list arguments) override __attribute__((format(printf, 3, 0)));
      virtual bool rejects(const LogMsgCategory& category) const
      {
        return false;
      }
    public:
      History history;
    };
    struct SubstituteException {};
    void testMaskingIntegration()
    {
      static constexpr const char* maskingConfigurationText = R"!!!(
        maskingPlugin:
          library: datamasker
          entryPoint: newPartialMaskSerialToken
          profile:
          - domain: 'urn:hpcc:unittest'
            property:
            - name: accepted
            valueType:
            - name: restricted
              maskStyle:
              - name: alternate
                pattern: +
              rule:
              - startToken: <foo>
                endToken: </foo>
                contentType: xml
              - startToken: <bar>
                endToken: </bar>
                contentType: json
            - name: '*'
              memberOf:
              - name: unconditional
              - name: 'sometimes-unconditional'
              maskStyle:
              - name: alternate
                pattern: '='
            - name: 'sometimes-restricted'
              memberOf:
              - name: sometimes
              - name: 'sometimes-unconditional'
              maskStyle:
              - name: alternate
                pattern: '?'
      )!!!";
      static constexpr const char * input = R"!!(<?xml version="1.0" encoding="UTF-8"?>
        <root>
          <Request>
            <foo>foo</foo>
            <bar>bar</bar>
            <baz>baz</baz>
          </Request>
        </root>
      )!!";
      static constexpr const char * traceScript = R"!!(<es:CustomRequestTransform xmlns:es="urn:hpcc:esdl:script" source="Request" target="Request">
        <es:trace-content select="."/>
        <es:trace-content select="." skip_mask="true()"/>
        <es:trace-content select="." content_type="xml"/>
        <es:trace-content select="." content_type="json"/>
        <es:trace-content select="." content_type="unrecognized"/>
        <es:trace-value select="foo" value_type="restricted"/>
        <es:trace-value select="bar" xpath_value_type="'restricted'" mask_style="alternate"/>
        <es:trace-value select="baz" value_type="unrecognized" class="information"/>
        <es:trace-value select="baz" value_type="unrecognized" class="progress"/>
        <es:trace-value select="baz" value_type="unrecognized" class="warning"/>
        <es:trace-value select="baz" value_type="unrecognized" class="error"/>
        <es:trace-value select="*" value_type="ambiguous"/>
        <es:trace-value select="bar" xpath_value_type="''"/>
        <es:trace-content select="maskValue(foo, 'restricted')" skip_mask="true()"/>
        <es:trace-content select="maskValue(bar, 'restricted', 'alternate')" skip_mask="true()"/>
        <es:trace-content select="maskValue(baz, 'restricted')" skip_mask="true()"/>
        <es:trace-content select="maskContent(toXmlString(foo))" skip_mask="true()"/>
        <es:trace-content select="maskContent(toXmlString(foo), 'xml')" skip_mask="true()"/>
        <es:trace-content select="maskContent(toXmlString(foo), 'json')" skip_mask="true()"/>
        <es:trace skip_mask="true()" select="getMaskValueBehavior('undefined')"/>
        <es:trace skip_mask="true()" select="getMaskValueBehavior('sometimes-restricted')"/>
        <es:trace skip_mask="true()" select="getMaskValueBehavior('sometimes-restricted', 'alternate')"/>
        <es:update-masking-context>
          <set name="valuetype-set" value="unconditional"/>
        </es:update-masking-context>
        <es:trace skip_mask="true()" select="getMaskValueBehavior('undefined')"/>
        <es:update-masking-context>
          <remove name="valuetype-set"/>
        </es:update-masking-context>
        <es:trace skip_mask="true()" select="getMaskValueBehavior('restricted')"/>
        <es:update-masking-context>
          <set name="valuetype-set" value="unconditional"/>
        </es:update-masking-context>
        <es:trace skip_mask="true()" select="getMaskValueBehavior('undefined', 'alternate')"/>
        <es:update-masking-context>
          <remove name="valuetype-set"/>
        </es:update-masking-context>
        <es:trace skip_mask="true()" select="getMaskValueBehavior('restricted', 'alternate')"/>
        <es:trace skip_mask="true()" select="getMaskingPropertyAwareness('unknown')"/>
        <es:trace skip_mask="true()" select="getMaskingPropertyAwareness('accepted')"/>
        <es:trace skip_mask="true()" select="getMaskingPropertyAwareness('valuetype-set')"/>
        <es:trace skip_mask="true()" select="canMaskContent()"/>
        <es:trace skip_mask="true()" select="canMaskContent('xml')"/>
        <es:trace skip_mask="true()" select="canMaskContent('json')"/>
        <es:trace skip_mask="true()" select="canMaskContent('yaml')"/>
        <!-- trace state scope -->
        <es:trace skip_mask="true()" select="isTraceEnabled()"/>
        <es:trace-options-scope enabled="false()">
          <es:trace skip_mask="true()" select="isTraceEnabled()"/>
          <es:set-trace-options enabled="true()"/>
          <es:trace skip_mask="true()" select="isTraceEnabled()"/>
          <es:set-trace-options enabled="false()"/>
          <es:trace skip_mask="true()" select="isTraceEnabled()"/>
        </es:trace-options-scope>
        <es:trace skip_mask="true()" select="isTraceEnabled()"/>
        <es:trace-options-scope enabled="false()" locked="true()">
          <es:trace skip_mask="true()" select="isTraceEnabled()"/>
          <es:set-trace-options enabled="true()"/>
          <es:trace skip_mask="true()" select="isTraceEnabled()"/>
          <es:set-trace-options enabled="false()"/>
          <es:trace skip_mask="true()" select="isTraceEnabled()"/>
        </es:trace-options-scope>
        <es:trace skip_mask="true()" select="isTraceEnabled()"/>
        <!-- masking context scope -->
        <es:trace-value select="foo" value_type="sometimes-restricted"/>
        <es:masking-context-scope>
          <es:update-masking-context>
            <set name="valuetype-set" value="sometimes"/>
          </es:update-masking-context>
          <es:trace-value select="foo" value_type="sometimes-restricted"/>
        </es:masking-context-scope>
        <es:trace-value select="foo" value_type="sometimes-restricted"/>
      </es:CustomRequestTransform>
      )!!";
      History expected({
        { MCuserInfo, "<Request><foo>***</foo><bar>***</bar><baz>baz</baz></Request>" },
        { MCuserInfo, "<Request><foo>foo</foo><bar>bar</bar><baz>baz</baz></Request>" },
        { MCuserInfo, "<Request><foo>***</foo><bar>bar</bar><baz>baz</baz></Request>" },
        { MCuserInfo, "<Request><foo>foo</foo><bar>***</bar><baz>baz</baz></Request>" },
        { MCuserInfo, "<Request><foo>foo</foo><bar>bar</bar><baz>baz</baz></Request>" },
        { MCuserInfo, "***" },
        { MCuserInfo, "+++" },
        { MCuserInfo, "baz" },
        { MCuserProgress, "baz" },
        { MCuserWarning, "baz" },
        { MCuserError, "baz" },
        { MCuserInfo, "***" },
        { MCuserInfo, "+++" },
        { MCuserInfo, "***" },
        { MCuserInfo, "<foo>***</foo>" },
        { MCuserInfo, "<foo>***</foo>" },
        { MCuserInfo, "<foo>foo</foo>" },
        { MCuserInfo, "0" },
        { MCuserInfo, "1" },
        { MCuserInfo, "3" },
        { MCuserInfo, "4" },
        { MCuserInfo, "5" },
        { MCuserInfo, "6" },
        { MCuserInfo, "7" },
        { MCuserInfo, "0" },
        { MCuserInfo, "1" },
        { MCuserInfo, "2" },
        { MCuserInfo, "true" },
        { MCuserInfo, "true" },
        { MCuserInfo, "true" },
        { MCuserInfo, "false" },
        // trace state scope
        { MCuserInfo, "true" },
        { MCuserInfo, "true" },
        { MCuserInfo, "true" },
        { MCuserInfo, "true" },
        // masking context scope
        { MCuserInfo, "foo" },
        { MCuserInfo, "***" },
        { MCuserInfo, "foo" },
      });

      try {
        Owned<IEspContext> ctx = createEspContext(nullptr);
        Owned<CMockTraceMsgSink> sink(new CMockTraceMsgSink());
        Owned<CModularTracer> tracer(createTracer(*sink));
        Owned<IDataMaskingEngine> engine(createMaskingEngine(maskingConfigurationText, *tracer));
        Owned<IEsdlScriptContext> scriptContext = createEsdlScriptContext(ctx, nullptr, LINK(engine));
        sink->history.clear();
        scriptContext->setTestMode(true);
        scriptContext->enableMasking(nullptr, 0);
        dynamic_cast<CModularTracer&>(scriptContext->tracerRef()).setSink(sink.getLink());
        scriptContext->setAttribute(ESDLScriptCtxSection_ESDLInfo, "service", "EsdlExample");
        scriptContext->setAttribute(ESDLScriptCtxSection_ESDLInfo, "method", "EchoPersonInfo");
        scriptContext->setAttribute(ESDLScriptCtxSection_ESDLInfo, "request_type", "EchoPersonInfoRequest");
        scriptContext->setAttribute(ESDLScriptCtxSection_ESDLInfo, "request", "EchoPersonInfoRequest");
        scriptContext->setContent(ESDLScriptCtxSection_ESDLRequest, input);

        runTransform(scriptContext, traceScript, ESDLScriptCtxSection_ESDLRequest, "Masking", "masking");

        // Only create the large message if we are going to fail the test
        if (sink->history.size() != expected.size())
        {
          VStringBuffer accumulatedMessage("Size mismatch- expected %ld, actual %ld", expected.size(), sink->history.size());
          unsigned idx = 0;
          accumulatedMessage.append("\nExpected:\n");
          for (const HistoricalEvent& he : expected)
          {
            VStringBuffer msg("    %2u: %d/%d/%s\n", ++idx, he.msgAudience, he.msgClass, he.msg.str());
            accumulatedMessage.append(msg);
          }
          idx = 0;
          accumulatedMessage.append("Actual:\n");
          for (const HistoricalEvent& he : sink->history)
          {
            VStringBuffer msg("    %2u: %d/%d/%s\n", ++idx, he.msgAudience, he.msgClass, he.msg.str());
            accumulatedMessage.append(msg);
          }
          CPPUNIT_FAIL(accumulatedMessage.str());
        }

        for (History::iterator aIt = sink->history.begin(), eIt = expected.begin(); eIt != expected.end(); ++aIt, ++eIt)
        {
          VStringBuffer comparison("\nExpected: %d/%d/%s\nActual:  %d/%d/%s\n", eIt->msgAudience, eIt->msgClass, eIt->msg.str(), aIt->msgAudience, aIt->msgClass, aIt->msg.str());
          CPPUNIT_ASSERT_MESSAGE(comparison.str(), *eIt == *aIt);
        }
      }
      catch (IException *E)
      {
        StringBuffer m;
        VStringBuffer exceptionMessage("Exception: code=%d, message=%s", E->errorCode(), E->errorMessage(m).str());
        E->Release();
        CPPUNIT_FAIL(exceptionMessage);
      }
    }
    IPTree* createMaskingConfiguration(const char* text)
    {
      Owned<IPTree> cfg;
      try
      {
        cfg.setown(createPTreeFromYAMLString(text));
      }
      catch (IException* E)
      {
        StringBuffer m;
        VStringBuffer exceptionMessage("Exception in createMaskingConfiguration: code=%d, message=%s", E->errorCode(), E->errorMessage(m).str());
        E->Release();
        CPPUNIT_FAIL(exceptionMessage);
      }
      return cfg.getClear();
    }
    IDataMaskingEngine* createMaskingEngine(const char* text, ITracer& tracer)
    {
      bool failed = false;
      Owned<IDataMaskingEngine> engine(new DataMasking::CEngine(LINK(&tracer)));
      Owned<IPTree> cfg(createMaskingConfiguration(text));
      Owned<IPTreeIterator> it(cfg->getElements("//maskingPlugin"));
      ForEach(*it)
        CPPUNIT_ASSERT_MESSAGE("createMaskingEngine: loadProfiles failed", engine->loadProfiles(it->query()));
      return engine.getClear();
    }
    CModularTracer* createTracer(CMockTraceMsgSink& sink)
    {
      Owned<CModularTracer> tracer(new CModularTracer());
      tracer->setSink(LINK(&sink));
      return tracer.getClear();
    }

    void testHTTPPostXml()
    {
        static constexpr const char * input = R"!!(<?xml version="1.0" encoding="UTF-8"?>
         <root>
            <Person>
               <FullName>
                  <First>Joe</First>
                  <ID>GI101</ID>
                  <ID>GI102</ID>
               </FullName>
            </Person>
          </root>
        )!!";

        static constexpr const char * script = R"!!(<es:CustomRequestTransform xmlns:es="urn:hpcc:esdl:script" target="Person">
            <es:variable name='value' select="'abc'"/>
            <es:http-post-xml url="'http://127.0.0.1:9876'" section="logging" name="roxiestuff">
              <es:content>
                <es:element name="Envelope">
                  <es:namespace prefix="soap" uri="http://schemas.xmlsoap.org/soap/envelope/" current="true" />
                  <es:element name="Body">
                    <es:element name="roxieechopersoninfoRequest">
                      <es:namespace uri="urn:hpccsystems:ecl:roxieechopersoninfo" current="true" />
                      <es:element name="roxieechopersoninforequest">
                        <es:element name="Row">
                          <es:element name="Name">
                            <es:set-value target="First" value="'aaa'"/>
                            <es:set-value target="Last" value="'bbb'"/>
                            <es:element name="Aliases">
                              <es:set-value target="Alias" value="'ccc'"/>
                              <es:add-value target="Alias" value="'ddd'"/>
                            </es:element>
                          </es:element>
                        </es:element>
                      </es:element>
                    </es:element>
                  </es:element>
                </es:element>
              </es:content>
            </es:http-post-xml>
            <es:element name="HttpPostStuff">
              <es:copy-of select="$roxiestuff"/>
            </es:element>
        </es:CustomRequestTransform>
        )!!";

        constexpr const char *config1 = R"!!(<config>
          <Transform>
            <Param name='testcase' value="new features"/>
          </Transform>
        </config>)!!";

            constexpr const char * result = R"!!(<root>
  <Person>
    <FullName>
      <First>Joe</First>
      <ID>GI101</ID>
      <ID>GI102</ID>
    </FullName>
    <HttpPostStuff>
      <roxiestuff>
        <request url="http://127.0.0.1:9876">
          <content>
            <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
              <soap:Body>
                <roxieechopersoninfoRequest xmlns="urn:hpccsystems:ecl:roxieechopersoninfo">
                  <roxieechopersoninforequest>
                    <Row>
                      <Name>
                        <First>aaa</First>
                        <Last>bbb</Last>
                        <Aliases>
                          <Alias>ccc</Alias>
                          <Alias>ddd</Alias>
                        </Aliases>
                      </Name>
                    </Row>
                  </roxieechopersoninforequest>
                </roxieechopersoninfoRequest>
              </soap:Body>
            </soap:Envelope>
          </content>
        </request>
        <response status="200 OK" error-code="0" content-type="text/xml">
          <content>
            <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
              <soap:Body>
                <roxieechopersoninfoResponse xmlns="urn:hpccsystems:ecl:roxieechopersoninfo" sequence="0">
                  <Results>
                    <Result>
                      <Dataset xmlns="urn:hpccsystems:ecl:roxieechopersoninfo:result:roxieechopersoninforesponse" name="RoxieEchoPersonInfoResponse">
                        <Row>
                          <Name>
                            <First>aaa</First>
                            <Last>bbb</Last>
                            <Aliases>
                              <Alias>ccc</Alias>
                              <Alias>ddd</Alias>
                            </Aliases>
                          </Name>
                          <Addresses/>
                        </Row>
                      </Dataset>
                    </Result>
                  </Results>
                </roxieechopersoninfoResponse>
              </soap:Body>
            </soap:Envelope>
          </content>
        </response>
      </roxiestuff>
    </HttpPostStuff>
  </Person>
</root>)!!";

        try {

            Owned<IEspContext> ctx = createEspContext(nullptr);
            Owned<IEsdlScriptContext> scriptContext = createTestScriptContext(ctx, input, config1);
            runTransform(scriptContext, script, ESDLScriptCtxSection_ESDLRequest, "MyResult", "http post xml");

            StringBuffer output;
            scriptContext->toXML(output, "MyResult");
            if (result)
            {
                VStringBuffer comparison("Expected: %s\nActual: %s", result, output.str());
                CPPUNIT_ASSERT_MESSAGE(comparison.str(), areEquivalentTestXMLStrings(result, output.str()));
            }
        }
        catch (IException *E)
        {
            StringBuffer m;
            VStringBuffer exceptionMessage("Exception: code=%d, message=%s", E->errorCode(), E->errorMessage(m).str());
            E->Release();
            CPPUNIT_FAIL(exceptionMessage);
        }
    }
    void testSynchronizeHTTPPostXml()
    {
        static constexpr const char * input = R"!!(<?xml version="1.0" encoding="UTF-8"?>
         <root>
            <Person>
               <FullName>
                  <First>Joe</First>
                  <ID>GI101</ID>
                  <ID>GI102</ID>
               </FullName>
            </Person>
          </root>
        )!!";

        static constexpr const char * script = R"!!(<es:CustomRequestTransform xmlns:es="urn:hpcc:esdl:script" target="Person">
            <es:variable name='value' select="'abc'"/>
            <es:synchronize max-at-once="3">
              <es:script>
                <es:element name="SyncBackgroundStuff">
                  <es:for-each select="es:tokenize('aaa,bbb,ccc,yyy', ',')">
                      <es:add-value target="value" value="."/>
                  </es:for-each>
                </es:element>
              </es:script>
              <es:http-post-xml url="'http://127.0.0.1:9876'" section="logging" name="roxiestuff1" test-delay="1000">
                <es:content>
                  <es:element name="Envelope">
                    <es:namespace prefix="soap" uri="http://schemas.xmlsoap.org/soap/envelope/" current="true" />
                    <es:element name="Body">
                      <es:element name="roxieechopersoninfoRequest">
                        <es:namespace uri="urn:hpccsystems:ecl:roxieechopersoninfo" current="true" />
                        <es:element name="roxieechopersoninforequest">
                          <es:element name="Row">
                            <es:element name="Name">
                              <es:set-value target="First" value="'aaa'"/>
                              <es:set-value target="Last" value="'bbb'"/>
                              <es:element name="Aliases">
                                <es:set-value target="Alias" value="'ccc'"/>
                                <es:add-value target="Alias" value="'ddd'"/>
                              </es:element>
                            </es:element>
                          </es:element>
                        </es:element>
                      </es:element>
                    </es:element>
                  </es:element>
                </es:content>
              </es:http-post-xml>
              <es:http-post-xml url="'http://127.0.0.1:9876'" section="logging" name="roxiestuff2" test-delay="1100">
                <es:content>
                  <es:element name="Envelope">
                    <es:namespace prefix="soap" uri="http://schemas.xmlsoap.org/soap/envelope/" current="true" />
                    <es:element name="Body">
                      <es:element name="roxieechopersoninfoRequest">
                        <es:namespace uri="urn:hpccsystems:ecl:roxieechopersoninfo" current="true" />
                        <es:element name="roxieechopersoninforequest">
                          <es:element name="Row">
                            <es:element name="Name">
                              <es:set-value target="First" value="'lll'"/>
                              <es:set-value target="Last" value="'mmm'"/>
                              <es:element name="Aliases">
                                <es:set-value target="Alias" value="'nnn'"/>
                                <es:add-value target="Alias" value="'ooo'"/>
                              </es:element>
                            </es:element>
                          </es:element>
                        </es:element>
                      </es:element>
                    </es:element>
                  </es:element>
                </es:content>
              </es:http-post-xml>
            </es:synchronize>
            <es:element name="HttpPostStuff">
              <es:copy-of select="$roxiestuff1"/>
              <es:copy-of select="$roxiestuff2"/>
            </es:element>
        </es:CustomRequestTransform>
        )!!";

        constexpr const char *config1 = R"!!(<config>
          <Transform>
            <Param name='testcase' value="new features"/>
          </Transform>
        </config>)!!";

        constexpr const char * result = R"!!(<root>
  <Person>
    <FullName>
      <First>Joe</First>
      <ID>GI101</ID>
      <ID>GI102</ID>
    </FullName>
    <SyncBackgroundStuff>
      <value>aaa</value>
      <value>bbb</value>
      <value>ccc</value>
      <value>yyy</value>
    </SyncBackgroundStuff>
    <HttpPostStuff>
      <roxiestuff1>
        <request url="http://127.0.0.1:9876">
          <content>
            <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
              <soap:Body>
                <roxieechopersoninfoRequest xmlns="urn:hpccsystems:ecl:roxieechopersoninfo">
                  <roxieechopersoninforequest>
                    <Row>
                      <Name>
                        <First>aaa</First>
                        <Last>bbb</Last>
                        <Aliases>
                          <Alias>ccc</Alias>
                          <Alias>ddd</Alias>
                        </Aliases>
                      </Name>
                    </Row>
                  </roxieechopersoninforequest>
                </roxieechopersoninfoRequest>
              </soap:Body>
            </soap:Envelope>
          </content>
        </request>
        <response status="200 OK" error-code="0" content-type="text/xml">
          <content>
            <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
              <soap:Body>
                <roxieechopersoninfoResponse xmlns="urn:hpccsystems:ecl:roxieechopersoninfo" sequence="0">
                  <Results>
                    <Result>
                      <Dataset xmlns="urn:hpccsystems:ecl:roxieechopersoninfo:result:roxieechopersoninforesponse" name="RoxieEchoPersonInfoResponse">
                        <Row>
                          <Name>
                            <First>aaa</First>
                            <Last>bbb</Last>
                            <Aliases>
                              <Alias>ccc</Alias>
                              <Alias>ddd</Alias>
                            </Aliases>
                          </Name>
                          <Addresses/>
                        </Row>
                      </Dataset>
                    </Result>
                  </Results>
                </roxieechopersoninfoResponse>
              </soap:Body>
            </soap:Envelope>
          </content>
        </response>
      </roxiestuff1>
      <roxiestuff2>
        <request url="http://127.0.0.1:9876">
          <content>
            <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
              <soap:Body>
                <roxieechopersoninfoRequest xmlns="urn:hpccsystems:ecl:roxieechopersoninfo">
                  <roxieechopersoninforequest>
                    <Row>
                      <Name>
                        <First>lll</First>
                        <Last>mmm</Last>
                        <Aliases>
                          <Alias>nnn</Alias>
                          <Alias>ooo</Alias>
                        </Aliases>
                      </Name>
                    </Row>
                  </roxieechopersoninforequest>
                </roxieechopersoninfoRequest>
              </soap:Body>
            </soap:Envelope>
          </content>
        </request>
        <response status="200 OK" error-code="0" content-type="text/xml">
          <content>
            <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
              <soap:Body>
                <roxieechopersoninfoResponse xmlns="urn:hpccsystems:ecl:roxieechopersoninfo" sequence="0">
                  <Results>
                    <Result>
                      <Dataset xmlns="urn:hpccsystems:ecl:roxieechopersoninfo:result:roxieechopersoninforesponse" name="RoxieEchoPersonInfoResponse">
                        <Row>
                          <Name>
                            <First>lll</First>
                            <Last>mmm</Last>
                            <Aliases>
                              <Alias>nnn</Alias>
                              <Alias>ooo</Alias>
                            </Aliases>
                          </Name>
                          <Addresses/>
                        </Row>
                      </Dataset>
                    </Result>
                  </Results>
                </roxieechopersoninfoResponse>
              </soap:Body>
            </soap:Envelope>
          </content>
        </response>
      </roxiestuff2>
    </HttpPostStuff>
  </Person>
</root>)!!";

        try {

            Owned<IEspContext> ctx = createEspContext(nullptr);
            Owned<IEsdlScriptContext> scriptContext = createTestScriptContext(ctx, input, config1);
            runTransform(scriptContext, script, ESDLScriptCtxSection_ESDLRequest, "MyResult", "synchronize");

            StringBuffer output;
            scriptContext->toXML(output, "MyResult");
            if (result)
            {
                VStringBuffer comparison("Expected: %s\nActual: %s", result, output.str());
                CPPUNIT_ASSERT_MESSAGE(comparison.str(), areEquivalentTestXMLStrings(result, output.str()));
            }
        }
        catch (IException *E)
        {
            StringBuffer m;
            VStringBuffer exceptionMessage("Exception: code=%d, message=%s", E->errorCode(), E->errorMessage(m).str());
            E->Release();
            CPPUNIT_FAIL(exceptionMessage);
        }
    }
    void testMysql()
    {
        constexpr const char *config1 = R"!!(<config>
          <Transform>
            <Param name='testcase' value="new features"/>
          </Transform>
        </config>)!!";

        static constexpr const char * data = R"!!(<?xml version="1.0" encoding="UTF-8"?>
        <root>
          <insert>
            <common_value>178</common_value>
            <common_r8>1.2</common_r8>
            <Row>
              <name>selected1</name>
              <bval>65</bval>
              <boolval>1</boolval>
              <r4>3.4</r4>
              <d>aa55aa55</d>
              <ddd>1234567.89</ddd>
              <u1>Straße1</u1>
              <u2>ᚠᛇᚻ᛫ᛒᛦᚦ᛫ᚠᚱᚩᚠᚢᚱ᛫ᚠᛁᚱᚪ᛫ᚷᛖᚻᚹᛦᛚᚳᚢᛗ</u2>
              <dt>2019-02-01 12:59:59</dt>
            </Row>
            <Row>
              <name>selected2</name>
              <bval>65</bval>
              <boolval>1</boolval>
              <r4>4.5</r4>
              <d>bb66bb66</d>
              <ddd>1234567.89</ddd>
              <u1>Straße3</u1>
              <u2>Straße4</u2>
              <dt>2019-02-01 13:59:59</dt>
            </Row>
            <Row>
              <name>selected3</name>
              <bval>65</bval>
              <boolval>1</boolval>
              <r4>5.6</r4>
              <d>cc77cc77</d>
              <ddd>1234567.89</ddd>
              <u1>Straße5</u1>
              <u2>色は匂へど 散りぬるを</u2>
              <dt>2019-02-01 14:59:59</dt>
            </Row>
          </insert>
          <cities>
            <name>aeiou</name>
            <name>aeou</name>
            <name>aoui</name>
            <name>ei</name>
            <name>aaa</name>
            <name>bbb</name>
          </cities>
          <read>
            <name>selected1</name>
            <name>selected3</name>
          </read>
        </root>
        )!!";

        static constexpr const char * input = R"!!(<?xml version="1.0" encoding="UTF-8"?>
        <root>
          <Person>
            <FullName>
              <First>Joe</First>
            </FullName>
          </Person>
        </root>
        )!!";

        static constexpr const char * script = R"!!(<es:CustomRequestTransform xmlns:es="urn:hpcc:esdl:script" target="Person">
          <es:variable name="secret" select="'mydb'"/>
          <es:variable name="database" select="'classicmodels'"/>
          <es:variable name="section" select="'sql'"/>
          <es:mysql secret="$secret" database="$database" section="$section" name="drop">
            <es:sql>DROP TABLE IF EXISTS tbl1;</es:sql>
          </es:mysql>
          <es:mysql secret="$secret" database="$database" section="$section" name="create">
            <es:sql>CREATE TABLE tbl1 ( name VARCHAR(20), bval BIT(15), value INT, boolval TINYINT, r8 DOUBLE, r4 FLOAT, d BLOB, ddd DECIMAL(10,2), u1 VARCHAR(10), u2 VARCHAR(10), dt DATETIME );</es:sql>
          </es:mysql>
          <es:mysql select="getDataSection('whatever')/this/insert/Row" secret="$secret" database="$database" section="$section" name="insert_each_row" resultset-tag="'inserted'">
            <es:bind name="name" value="name"/>
            <es:bind name="bval" value="bval" type="BIT(15)"/>
            <es:bind name="value" value="../common_value"/>
            <es:bind name="boolval" value="boolval"/>
            <es:bind name="r8" value="../common_r8"/>
            <es:bind name="r4" value="r4"/>
            <es:bind name="d" value="d"/>
            <es:bind name="ddd" value="ddd"/>
            <es:bind name="u1" value="u1"/>
            <es:bind name="u2" value="u2"/>
            <es:bind name="dt" value="dt"/>
            <es:sql>INSERT INTO tbl1 values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);</es:sql>
          </es:mysql>
          <es:mysql secret="$secret" database="$database" section="$section" name="drop">
            <es:sql>DROP TABLE IF EXISTS tblcities;</es:sql>
          </es:mysql>
          <es:mysql secret="$secret" database="$database" section="$section" name="createcites">
            <es:sql>CREATE TABLE tblcities ( city VARCHAR(20) );</es:sql>
          </es:mysql>
          <es:mysql select="getDataSection('whatever')/this/cities/name" secret="$secret" database="$database" section="$section" name="insert_each_row" resultset-tag="'inserted'">
            <es:bind name="city" value="."/>
            <es:sql>INSERT INTO tblcities values (?);</es:sql>
          </es:mysql>
          <es:mysql select="getDataSection('whatever')/this/read/name" secret="$secret" database="$database" section="$section" name="select_each_name" resultset-tag="'selected'">
            <es:bind name="name" value="."/>
            <es:sql>SELECT * FROM tbl1 where name = ?;</es:sql>
          </es:mysql>
          <es:mysql secret="$secret" database="$database" section="$section" name="mysql_session_info" MYSQL_SET_CHARSET_NAME="'latin1'">
            <es:sql>
            SELECT * FROM performance_schema.session_variables
            WHERE VARIABLE_NAME IN (
            'character_set_client', 'character_set_connection',
            'character_set_results', 'collation_connection'
            ) ORDER BY VARIABLE_NAME;
            </es:sql>
          </es:mysql>
          <es:mysql secret="$secret" database="$database" section="$section" name="select_all" resultset-tag="'onecall'">
            <es:sql>SELECT * FROM tbl1;</es:sql>
          </es:mysql>
          <es:for-each select="$select_all/onecall/Row">
            <es:element name="r">
              <es:copy-of select="*"/>
            </es:element>
          </es:for-each>
            <es:mysql secret="$secret" database="$database" section="$section" name="mysqlresult">
              <es:sql>SELECT * FROM tblcities;</es:sql>
            </es:mysql>
            <es:variable name="i" select="$mysqlresult/Row/city[contains(.,'i')]" />
            <es:variable name="e" select="$mysqlresult/Row/city[contains(.,'e')]" />
            <es:ensure-target xpath="iii">
                <es:copy-of select="$i" />
            </es:ensure-target>
            <es:ensure-target xpath="eee">
                <es:copy-of select="$e" />
            </es:ensure-target>
            <es:ensure-target xpath="cities">
              <es:for-each select="set:intersection($i, $e)">
                <es:copy-of select="." />
              </es:for-each>
            </es:ensure-target>
        </es:CustomRequestTransform>
        )!!";

        Owned<IEspContext> ctx = createEspContext(nullptr);
        Owned<IEsdlScriptContext> scriptContext = createTestScriptContext(ctx, input, config1);
        scriptContext->appendContent("whatever", "this", data);

        try
        {
            runTransform(scriptContext, script, ESDLScriptCtxSection_ESDLRequest, "MyResult", "http post xml");
        }
        catch (IException *E)
        {
            StringBuffer m;
            VStringBuffer exceptionMessage("Exception: code=%d, message=%s", E->errorCode(), E->errorMessage(m).str());
            E->Release();
            CPPUNIT_FAIL(exceptionMessage);
        }

        StringBuffer output;
        scriptContext->toXML(output);
        DBGLOG("testMysql output: /n%s", output.str());
    }

    void testScriptMap()
    {
        constexpr const char * serverScripts = R"!!(<Transforms xmlns:es='urn:hpcc:esdl:script'>
      <es:BackendRequest>
        <es:set-value target="Row/Name/First" value="'modified-request-at-service'" />
      </es:BackendRequest>
      <es:BackendRequest>
        <es:set-value target="BRSRV2" value="'s2'" />
      </es:BackendRequest>
      <es:BackendRequest>
        <es:set-value target="BRSRV3" value="'s3'" />
      </es:BackendRequest>
      <es:BackendResponse xmlns:resp="urn:hpccsystems:ecl:roxieechopersoninfo" xmlns:ds1="urn:hpccsystems:ecl:roxieechopersoninfo:result:roxieechopersoninforesponse">
        <es:target xpath="soap:Body">
          <es:target xpath="resp:roxieechopersoninfoResponse">
            <es:target xpath="resp:Results/resp:Result">
              <es:target xpath="ds1:Dataset[@name='RoxieEchoPersonInfoResponse']">
                <es:set-value target="ds1:Row/ds1:Name/ds1:Last" value="'modified-response-at-service'" />
              </es:target>
            </es:target>
          </es:target>
        </es:target>
      </es:BackendResponse>
      <es:BackendResponse>
        <es:set-value target="BRESPSRV2" value="'s22'" />
      </es:BackendResponse>
      <es:BackendResponse>
        <es:set-value target="BRESPSRV3" value="'s33'" />
      </es:BackendResponse>
      <es:PreLogging>
        <es:set-value target="PLSRV1" value="'s111'" />
      </es:PreLogging>
      <es:PreLogging>
        <es:set-value target="PLSRV2" value="'s222'" />
      </es:PreLogging>
      <es:PreLogging>
        <es:set-value target="PLSRV3" value="'s333'" />
      </es:PreLogging>
  </Transforms>)!!";

        constexpr const char * methodScripts = R"!!(<Transforms xmlns:es='urn:hpcc:esdl:script'>
  <es:BackendRequest>
    <es:append-to-value target="Row/Name/First" value="'-and-method'" />
  </es:BackendRequest>
  <es:BackendRequest>
    <es:set-value target="BRMTH2" value="'m2'" />
  </es:BackendRequest>
  <es:BackendRequest>
    <es:set-value target="BRMTH3" value="'m3'" />
  </es:BackendRequest>
  <es:BackendResponse xmlns:resp="urn:hpccsystems:ecl:roxieechopersoninfo" xmlns:ds1="urn:hpccsystems:ecl:roxieechopersoninfo:result:roxieechopersoninforesponse">
    <es:target xpath="soap:Body">
      <es:target xpath="resp:roxieechopersoninfoResponse">
        <es:target xpath="resp:Results/resp:Result">
          <es:target xpath="ds1:Dataset[@name='RoxieEchoPersonInfoResponse']">
            <es:append-to-value target="ds1:Row/ds1:Name/ds1:Last" value="'-and-method'" />
          </es:target>
        </es:target>
      </es:target>
    </es:target>
  </es:BackendResponse>
  <es:BackendResponse xmlns:resp="urn:hpccsystems:ecl:roxieechopersoninfo" xmlns:ds1="urn:hpccsystems:ecl:roxieechopersoninfo:result:roxieechopersoninforesponse">
    <es:http-post-xml url="'http://127.0.0.1:9876'" section="logdata/LogDataset" name="roxie_call_success">
      <es:content>
        <es:element name="Envelope">
          <es:namespace prefix="soap" uri="http://schemas.xmlsoap.org/soap/envelope/" current="true" />
          <es:element name="Body">
            <es:element name="roxieechopersoninfoRequest">
              <es:namespace uri="urn:hpccsystems:ecl:roxieechopersoninfo" current="true" />
              <es:element name="roxieechopersoninforequest">
                <es:element name="Row">
                  <es:element name="Name">
                    <es:set-value target="First" value="'echoFirst'"/>
                    <es:set-value target="Last" value="'echoLast'"/>
                    <es:element name="Aliases">
                      <es:set-value target="Alias" value="'echoA1'"/>
                      <es:add-value target="Alias" value="'echoA2'"/>
                    </es:element>
                  </es:element>
                </es:element>
              </es:element>
            </es:element>
          </es:element>
        </es:element>
      </es:content>
    </es:http-post-xml>
    <es:target xpath="soap:Body">
    <es:target xpath="resp:roxieechopersoninfoResponse">
    <es:target xpath="resp:Results/resp:Result">
    <es:target xpath="ds1:Dataset[@name='RoxieEchoPersonInfoResponse']">
        <es:source xpath="$roxie_call_success/response/content">
          <es:source xpath="soap:Envelope/soap:Body">
            <es:source xpath="resp:roxieechopersoninfoResponse/resp:Results/resp:Result">
              <es:source xpath="ds1:Dataset/ds1:Row">
                <es:append-to-value target="ds1:Row/ds1:Name/ds1:Last" value="concat('-plus-echoed-alias-', ds1:Name/ds1:Aliases/ds1:Alias[2])" />
              </es:source>
            </es:source>
          </es:source>
        </es:source>
    </es:target>
    </es:target>
    </es:target>
    </es:target>
  </es:BackendResponse>
  <es:BackendResponse>
    <es:set-value target="BRESPMTH3" value="'m33'" />
  </es:BackendResponse>
  <es:PreLogging>
    <es:http-post-xml url="'http://127.0.0.1:9876'" section="logdata/LogDatasets" name="roxie_call_exception">
      <es:content>
        <es:element name="Envelope">
          <es:namespace prefix="soap" uri="http://schemas.xmlsoap.org/soap/envelope/" current="true" />
          <es:element name="Body">
            <es:element name="nonexistent_query">
              <es:namespace uri="urn:hpccsystems:ecl:roxieechopersoninfo" current="true" />
              <es:element name="nonexistent_queryrequest">
                <es:element name="Row">
                  <es:element name="Name">
                    <es:set-value target="First" value="'aaa'"/>
                    <es:set-value target="Last" value="'bbb'"/>
                    <es:element name="Aliases">
                      <es:set-value target="Alias" value="'ccc'"/>
                      <es:set-value target="Alias" value="'ddd'"/>
                    </es:element>
                  </es:element>
                </es:element>
              </es:element>
            </es:element>
          </es:element>
        </es:element>
      </es:content>
    </es:http-post-xml>
  </es:PreLogging>
  <es:PreLogging>
    <es:set-value target="PLMTH2" value="'m222'" />
  </es:PreLogging>
  <es:PreLogging>
    <es:set-value target="PLMTH3" value="'m333'" />
  </es:PreLogging>
  </Transforms>)!!";

        constexpr const char * input = R"!!(<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
 <soap:Body>
  <roxieechopersoninfoResponse xmlns="urn:hpccsystems:ecl:roxieechopersoninfo" sequence="0">
   <Results>
    <Result>
     <Dataset xmlns="urn:hpccsystems:ecl:roxieechopersoninfo:result:roxieechopersoninforesponse" name="RoxieEchoPersonInfoResponse">
      <Row>
       <Name>
        <First>aaa</First>
        <Last>bbbb</Last>
        <Aliases>
         <Alias>a</Alias>
         <Alias>b</Alias>
         <Alias>c</Alias>
        </Aliases>
       </Name>
       <Addresses>
        <Address>
         <Line1>111</Line1>
         <Line2>222</Line2>
         <City>Boca Raton</City>
         <State>FL</State>
         <Zip>33487</Zip>
         <type>ttt</type>
        </Address>
       </Addresses>
      </Row>
     </Dataset>
    </Result>
   </Results>
  </roxieechopersoninfoResponse>
 </soap:Body>
</soap:Envelope>)!!";

        constexpr const char *config1 = R"!!(<config>
          <Transform>
            <Param name='testcase' value="transform map"/>
          </Transform>
        </config>)!!";

        constexpr const char * result = R"!!(<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <roxieechopersoninfoResponse xmlns="urn:hpccsystems:ecl:roxieechopersoninfo" sequence="0">
      <Results>
        <Result>
          <Dataset xmlns="urn:hpccsystems:ecl:roxieechopersoninfo:result:roxieechopersoninforesponse" name="RoxieEchoPersonInfoResponse">
            <Row>
              <Name>
                <First>aaa</First>
                <Last>modified-response-at-service-and-method-plus-echoed-alias-echoA2</Last>
                <Aliases>
                  <Alias>a</Alias>
                  <Alias>b</Alias>
                  <Alias>c</Alias>
                </Aliases>
              </Name>
              <Addresses>
                <Address>
                  <Line1>111</Line1>
                  <Line2>222</Line2>
                  <City>Boca Raton</City>
                  <State>FL</State>
                  <Zip>33487</Zip>
                  <type>ttt</type>
                </Address>
              </Addresses>
            </Row>
          </Dataset>
        </Result>
      </Results>
    </roxieechopersoninfoResponse>
  </soap:Body>
  <BRESPSRV2>s22</BRESPSRV2>
  <BRESPSRV3>s33</BRESPSRV3>
  <BRESPMTH3>m33</BRESPMTH3>
</soap:Envelope>)!!";

      Owned<IEspContext> ctx = createEspContext(nullptr);
      Owned<IEsdlScriptContext> scriptContext = createTestScriptContext(ctx, input, config1);

      bool legacy = false;
      Owned<IEsdlTransformMethodMap> map = createEsdlTransformMethodMap();
      map->addMethodTransforms("", serverScripts, legacy);
      map->addMethodTransforms("mymethod", methodScripts, legacy);

      IEsdlTransformSet *serviceSet = map->queryMethodEntryPoint("", "BackendResponse");
      IEsdlTransformSet *methodSet = map->queryMethodEntryPoint("mymethod", "BackendResponse");

      scriptContext->setContent(ESDLScriptCtxSection_InitialResponse, input);

      processServiceAndMethodTransforms(scriptContext, {serviceSet, methodSet}, ESDLScriptCtxSection_InitialResponse, "MyResult");
      StringBuffer output;
      scriptContext->toXML(output, "MyResult");
      if (result)
      {
          VStringBuffer comparison("Expected: %s\nActual: %s", result, output.str());
          CPPUNIT_ASSERT_MESSAGE(comparison.str(), areEquivalentTestXMLStrings(result, output.str()));
      }
    }

    void testCallFunctions()
    {
        constexpr const char * serviceScripts = R"!!(<Transforms xmlns:es='urn:hpcc:esdl:script'>
      <es:Functions>
        <es:function name="inServiceCommonFunction">
          <es:param name="paramCommonInService1" select="'paramCommonInService1-fail-using-default'"/>
          <es:param name="paramCommonInService2" select="'paramCommonInService2-success-using-default'"/>
          <es:set-value target="ds1:Row/ds1:InServiceCommonParam1" value="$paramCommonInService1" />
          <es:set-value target="ds1:Row/ds1:InServiceCommonParam2" value="$paramCommonInService2" />
        </es:function>
        <es:function name="inServiceCommonFunction2">
          <es:set-value target="ds1:Row/ds1:ServiceCommonFunctionFromService" value="'service-common-instance-called-from-service'" />
        </es:function>
        <es:function name="inServiceLocalFunction">
          <es:set-value target="ds1:Row/ds1:InServiceLocalInheritanceError" value="'service-common-instance-called'" />
        </es:function>
        <es:function name="inMethodCommonFunction">
          <es:set-value target="ds1:Row/ds1:InMethodCommonInheritanceError" value="'service-common-instance-called'" />
        </es:function>
      </es:Functions>
      <es:BackendResponse xmlns:resp="urn:hpccsystems:ecl:roxieechopersoninfo" xmlns:ds1="urn:hpccsystems:ecl:roxieechopersoninfo:result:roxieechopersoninforesponse">
        <es:variable name="paramLocalInService1" select="'paramLocalInService1-fail-from-top-o-the-script'"/>
        <es:variable name="paramLocalInService2" select="'paramLocalInService2-fail-from-top-o-the-script'"/>
        <es:function name="inServiceLocalFunction">
          <es:param name="paramLocalInService1" select="'paramLocalInService1-fail-using-default'"/>
          <es:param name="paramLocalInService2" select="'paramLocalInService2-success-using-default'"/>
          <es:set-value target="ds1:Row/ds1:InServiceLocalParam1" value="$paramLocalInService1" />
          <es:set-value target="ds1:Row/ds1:InServiceLocalParam2" value="$paramLocalInService2" />
        </es:function>
        <es:target xpath="soap:Body">
          <es:target xpath="resp:roxieechopersoninfoResponse">
            <es:target xpath="resp:Results/resp:Result">
              <es:target xpath="ds1:Dataset[@name='RoxieEchoPersonInfoResponse']">
              <es:call-function name="inServiceLocalFunction">
                <es:with-param name="paramLocalInService1" select="'paramLocalInService1-success-with-param'"/>
              </es:call-function>
              <es:call-function name="inMethodCommonFunction2"/>
              <es:call-function name="inServiceCommonFunction2"/>
                <es:set-value target="ds1:Row/ds1:Name/ds1:Last" value="'modified-response-at-service'" />
              </es:target>
            </es:target>
          </es:target>
        </es:target>
      </es:BackendResponse>
      <es:BackendResponse>
        <es:set-value target="BRESPSRV2" value="'s22'" />
      </es:BackendResponse>
      <es:BackendResponse>
        <es:set-value target="BRESPSRV3" value="'s33'" />
      </es:BackendResponse>
  </Transforms>)!!";

        constexpr const char * methodScripts = R"!!(<Transforms xmlns:es='urn:hpcc:esdl:script'>
  <es:Functions>
    <es:function name="inMethodCommonFunction">
      <es:param name="paramCommonInMethod1" select="'paramCommonInMethod1-fail-using-default'"/>
      <es:param name="paramCommonInMethod2" select="'paramCommonInMethod2-success-using-default'"/>
      <es:set-value target="ds1:Row/ds1:InMethodCommonParam1" value="$paramCommonInMethod1" />
      <es:set-value target="ds1:Row/ds1:InMethodCommonParam2" value="$paramCommonInMethod2" />
    </es:function>
    <es:function name="inMethodLocalFunction">
      <es:set-value target="ds1:Row/ds1:InMethodLocalInheritanceError" value="'method-common-instance-called'" />
    </es:function>
    <es:function name="inMethodCommonFunction2">
      <es:set-value target="ds1:Row/ds1:MethodCommonFunctionFromService" value="'method-common-instance-called-from-service'" />
    </es:function>
  </es:Functions>
  <es:BackendResponse xmlns:resp="urn:hpccsystems:ecl:roxieechopersoninfo" xmlns:ds1="urn:hpccsystems:ecl:roxieechopersoninfo:result:roxieechopersoninforesponse">
    <es:variable name="paramLocalInMethod1" select="'paramLocalInMethod1-fail-from-top-o-the-script'"/>
    <es:variable name="paramLocalInMethod2" select="'paramLocalInMethod2-fail-from-top-o-the-script'"/>
    <es:function name="inMethodLocalFunction">
      <es:param name="paramLocalInMethod1" select="'paramLocalInMethod1-fail-using-default'"/>
      <es:param name="paramLocalInMethod2" select="'paramLocalInMethod2-success-using-default'"/>
      <es:set-value target="ds1:Row/ds1:InMethodLocalParam1" value="$paramLocalInMethod1" />
      <es:set-value target="ds1:Row/ds1:InMethodLocalParam2" value="$paramLocalInMethod2" />
    </es:function>
    <es:target xpath="soap:Body">
      <es:target xpath="resp:roxieechopersoninfoResponse">
        <es:target xpath="resp:Results/resp:Result">
          <es:target xpath="ds1:Dataset[@name='RoxieEchoPersonInfoResponse']">
            <es:variable name="paramLocalInMethod1" select="'paramLocalInMethod1-fail-from-just-above'"/>
            <es:variable name="paramLocalInMethod2" select="'paramLocalInMethod2-fail-from-just-above'"/>
            <es:append-to-value target="ds1:Row/ds1:Name/ds1:Last" value="'-and-method'" />
            <es:call-function name="inMethodLocalFunction">
              <es:with-param name="paramLocalInMethod1" select="'paramLocalInMethod1-success-with-param'"/>
            </es:call-function>
            <es:call-function name="inMethodCommonFunction">
              <es:with-param name="paramCommonInMethod1" select="'paramCommonInMethod1-success-with-param'"/>
            </es:call-function>
            <es:call-function name="inServiceCommonFunction">
              <es:with-param name="paramCommonInService1" select="'paramCommonlInService1-success-with-param'"/>
            </es:call-function>
          </es:target>
        </es:target>
      </es:target>
    </es:target>
  </es:BackendResponse>
  <es:BackendResponse xmlns:resp="urn:hpccsystems:ecl:roxieechopersoninfo" xmlns:ds1="urn:hpccsystems:ecl:roxieechopersoninfo:result:roxieechopersoninforesponse">
    <es:http-post-xml url="'http://127.0.0.1:9876'" section="logdata/LogDataset" name="roxie_call_success">
      <es:content>
        <es:element name="Envelope">
          <es:namespace prefix="soap" uri="http://schemas.xmlsoap.org/soap/envelope/" current="true" />
          <es:element name="Body">
            <es:element name="roxieechopersoninfoRequest">
              <es:namespace uri="urn:hpccsystems:ecl:roxieechopersoninfo" current="true" />
              <es:element name="roxieechopersoninforequest">
                <es:element name="Row">
                  <es:element name="Name">
                    <es:set-value target="First" value="'echoFirst'"/>
                    <es:set-value target="Last" value="'echoLast'"/>
                    <es:element name="Aliases">
                      <es:set-value target="Alias" value="'echoA1'"/>
                      <es:add-value target="Alias" value="'echoA2'"/>
                    </es:element>
                  </es:element>
                </es:element>
              </es:element>
            </es:element>
          </es:element>
        </es:element>
      </es:content>
    </es:http-post-xml>
    <es:target xpath="soap:Body">
    <es:target xpath="resp:roxieechopersoninfoResponse">
    <es:target xpath="resp:Results/resp:Result">
    <es:target xpath="ds1:Dataset[@name='RoxieEchoPersonInfoResponse']">
        <es:source xpath="$roxie_call_success/response/content">
          <es:source xpath="soap:Envelope/soap:Body">
            <es:source xpath="resp:roxieechopersoninfoResponse/resp:Results/resp:Result">
              <es:source xpath="ds1:Dataset/ds1:Row">
                <es:append-to-value target="ds1:Row/ds1:Name/ds1:Last" value="concat('-plus-echoed-alias-', ds1:Name/ds1:Aliases/ds1:Alias[2])" />
              </es:source>
            </es:source>
          </es:source>
        </es:source>
    </es:target>
    </es:target>
    </es:target>
    </es:target>
  </es:BackendResponse>
  <es:BackendResponse>
    <es:set-value target="BRESPMTH3" value="'m33'" />
  </es:BackendResponse>
  </Transforms>)!!";

        constexpr const char * input = R"!!(<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
 <soap:Body>
  <roxieechopersoninfoResponse xmlns="urn:hpccsystems:ecl:roxieechopersoninfo" sequence="0">
   <Results>
    <Result>
     <Dataset xmlns="urn:hpccsystems:ecl:roxieechopersoninfo:result:roxieechopersoninforesponse" name="RoxieEchoPersonInfoResponse">
      <Row>
       <Name>
        <First>aaa</First>
        <Last>bbbb</Last>
        <Aliases>
         <Alias>a</Alias>
         <Alias>b</Alias>
         <Alias>c</Alias>
        </Aliases>
       </Name>
       <Addresses>
        <Address>
         <Line1>111</Line1>
         <Line2>222</Line2>
         <City>Boca Raton</City>
         <State>FL</State>
         <Zip>33487</Zip>
         <type>ttt</type>
        </Address>
       </Addresses>
      </Row>
     </Dataset>
    </Result>
   </Results>
  </roxieechopersoninfoResponse>
 </soap:Body>
</soap:Envelope>)!!";

        constexpr const char *config1 = R"!!(<config>
          <Transform>
            <Param name='testcase' value="transform map"/>
          </Transform>
        </config>)!!";

        constexpr const char * result = R"!!(<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <roxieechopersoninfoResponse xmlns="urn:hpccsystems:ecl:roxieechopersoninfo" sequence="0">
      <Results>
        <Result>
          <Dataset xmlns="urn:hpccsystems:ecl:roxieechopersoninfo:result:roxieechopersoninforesponse" name="RoxieEchoPersonInfoResponse">
            <Row>
              <Name>
                <First>aaa</First>
                <Last>modified-response-at-service-and-method-plus-echoed-alias-echoA2</Last>
                <Aliases>
                  <Alias>a</Alias>
                  <Alias>b</Alias>
                  <Alias>c</Alias>
                </Aliases>
              </Name>
              <Addresses>
                <Address>
                  <Line1>111</Line1>
                  <Line2>222</Line2>
                  <City>Boca Raton</City>
                  <State>FL</State>
                  <Zip>33487</Zip>
                  <type>ttt</type>
                </Address>
              </Addresses>
              <InServiceLocalParam1>paramLocalInService1-success-with-param</InServiceLocalParam1>
              <InServiceLocalParam2>paramLocalInService2-success-using-default</InServiceLocalParam2>
              <MethodCommonFunctionFromService>method-common-instance-called-from-service</MethodCommonFunctionFromService>
              <ServiceCommonFunctionFromService>service-common-instance-called-from-service</ServiceCommonFunctionFromService>
              <InMethodLocalParam1>paramLocalInMethod1-success-with-param</InMethodLocalParam1>
              <InMethodLocalParam2>paramLocalInMethod2-success-using-default</InMethodLocalParam2>
              <InMethodCommonParam1>paramCommonInMethod1-success-with-param</InMethodCommonParam1>
              <InMethodCommonParam2>paramCommonInMethod2-success-using-default</InMethodCommonParam2>
              <InServiceCommonParam1>paramCommonlInService1-success-with-param</InServiceCommonParam1>
              <InServiceCommonParam2>paramCommonInService2-success-using-default</InServiceCommonParam2>
            </Row>
          </Dataset>
        </Result>
      </Results>
    </roxieechopersoninfoResponse>
  </soap:Body>
  <BRESPSRV2>s22</BRESPSRV2>
  <BRESPSRV3>s33</BRESPSRV3>
  <BRESPMTH3>m33</BRESPMTH3>
</soap:Envelope>)!!";
      try
      {
        Owned<IEspContext> ctx = createEspContext(nullptr);

        bool legacy = false;
        Owned<IEsdlTransformMethodMap> map = createEsdlTransformMethodMap();
        map->addMethodTransforms("", serviceScripts, legacy);
        map->addMethodTransforms("mymethod", methodScripts, legacy);
        map->bindFunctionCalls();

        IEsdlTransformSet *serviceSet = map->queryMethodEntryPoint("", "BackendResponse");
        IEsdlTransformSet *methodSet = map->queryMethodEntryPoint("mymethod", "BackendResponse");

        Owned<IEsdlScriptContext> scriptContext = createTestScriptContext(ctx, input, config1, map->queryFunctionRegister("mymethod"));
        scriptContext->setContent(ESDLScriptCtxSection_InitialResponse, input);

        processServiceAndMethodTransforms(scriptContext, {serviceSet, methodSet}, ESDLScriptCtxSection_InitialResponse, "MyResult");
        StringBuffer output;
        scriptContext->toXML(output, "MyResult");
        if (result)
        {
            VStringBuffer comparison("Expected: %s\nActual: %s", result, output.str());
            CPPUNIT_ASSERT_MESSAGE(comparison.str(), areEquivalentTestXMLStrings(result, output.str(), true));
        }
      }
      catch(IException *E)
      {
          StringBuffer m;
          VStringBuffer exceptionMessage("Exception: code=%d, message=%s", E->errorCode(), E->errorMessage(m).str());
          E->Release();
          CPPUNIT_FAIL(exceptionMessage);
      }
    }
};

inline void ESDLTests::CMockTraceMsgSink::valog(const LogMsgCategory& category, const char* format, va_list arguments)
{
  history.emplace_back(category, format, arguments);
}

CPPUNIT_TEST_SUITE_REGISTRATION( ESDLTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( ESDLTests, "ESDL" );

#endif // _USE_CPPUNIT
