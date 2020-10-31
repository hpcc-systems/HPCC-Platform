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

#include <stdio.h>

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

bool areEquivalentTestXMLStrings(const char *xml1, const char *xml2)
{
    if (isEmptyString(xml1) || isEmptyString(xml2))
        return false;
    Owned<IPropertyTree> tree1 = createPTreeFromXMLString(xml1);
    Owned<IPropertyTree> tree2 = createPTreeFromXMLString(xml2);
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
      //CPPUNIT_TEST(testScriptMap); //requires a particular roxie query
      //CPPUNIT_TEST(testHTTPPostXml); //requires a particular roxie query
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

    IEsdlScriptContext *createTestScriptContext(IEspContext *ctx, const char *xml, const char *config)
    {
        Owned<IEsdlScriptContext> scriptContext = createEsdlScriptContext(ctx);
        scriptContext->setAttribute(ESDLScriptCtxSection_ESDLInfo, "service", "EsdlExample");
        scriptContext->setAttribute(ESDLScriptCtxSection_ESDLInfo, "method", "EchoPersonInfo");
        scriptContext->setAttribute(ESDLScriptCtxSection_ESDLInfo, "request_type", "EchoPersonInfoRequest");
        scriptContext->setAttribute(ESDLScriptCtxSection_ESDLInfo, "request", "EchoPersonInfoRequest");

        scriptContext->setContent(ESDLScriptCtxSection_BindingConfig, config);
        scriptContext->setContent(ESDLScriptCtxSection_TargetConfig, target_config);
        scriptContext->setContent(ESDLScriptCtxSection_ESDLRequest, xml);
        return scriptContext.getClear();
    }

    void runTransform(IEsdlScriptContext *scriptContext, const char *scriptXml, const char *srcSection, const char *tgtSection, const char *testname, int code)
    {
        Owned<IEsdlCustomTransform> tf = createEsdlCustomTransform(scriptXml, nullptr);

        tf->processTransform(scriptContext, srcSection, tgtSection);
        if (code)
            throw MakeStringException(99, "Test failed(%s): expected an explicit exception %d", testname, code);
    }
    void runTest(const char *testname, const char *scriptXml, const char *xml, const char *config, const char *result, int code)
    {
        try
        {
            //printf("starting %s:\n", testname);  //uncomment to help debug
            Owned<IEspContext> ctx = createEspContext(nullptr);
            Owned<IEsdlScriptContext> scriptContext = createTestScriptContext(ctx, xml, config);
            runTransform(scriptContext, scriptXml, ESDLScriptCtxSection_ESDLRequest, ESDLScriptCtxSection_FinalRequest, testname, code);

            StringBuffer output;
            scriptContext->toXML(output.clear(), ESDLScriptCtxSection_FinalRequest);


            if (result && !areEquivalentTestXMLStrings(result, output.str()))
            {
                fputs(output.str(), stdout);
                fflush(stdout);
                fprintf(stdout, "\nTest failed(%s)\n", testname);
                CPPUNIT_ASSERT(false);
            }
        }
        catch (IException *E)
        {
            StringBuffer m;
            if (code!=E->errorCode())
            {
                StringBuffer m;
                fprintf(stdout, "\nTest(%s) Expected %d Exception %d - %s\n", testname, code, E->errorCode(), E->errorMessage(m).str());
                E->Release();
                CPPUNIT_ASSERT(false);
            }
            E->Release();
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

      runTest("implicit-prefix", esdlImplicitNamespaceSelectPath, soapRequestImplicitPrefix, config, implicitPrefixResult, 0);

      // The implicit 'n' prefix is required if the content has a namespace defined
      // with no prefix. This test is expected to throw an exception.
      runTest("implicit-prefix-not-used", esdlImplicitNamespaceSelectPath, soapRequestImplicitPrefix, configNoPrefix, implicitPrefixResult, 99);
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
              <es:remove-node target="Name/ID[1]"/> //removing first one changes the index count so each is 1
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
            runTransform(scriptContext, script, ESDLScriptCtxSection_ESDLRequest, "FirstPass", "script context 1", 0);

            scriptContext->setContent(ESDLScriptCtxSection_BindingConfig, config2);
            runTransform(scriptContext, script, "FirstPass", "SecondPass", "script context 2", 0);

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
            if (result && !areEquivalentTestXMLStrings(result, output.str()))
            {
                fputs(output.str(), stdout);
                fflush(stdout);
                throw MakeStringException(100, "Test failed(%s)", "script context");
            }
        }
        catch (IException *E)
        {
            StringBuffer m;
            fprintf(stdout, "\nTest(%s) Exception %d - %s\n", "script context", E->errorCode(), E->errorMessage(m).str());
            E->Release();
            CPPUNIT_ASSERT(false);
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
            runTransform(scriptContext, script, ESDLScriptCtxSection_ESDLRequest, "FirstPass", "target element 1", 0);

            StringBuffer output;
            scriptContext->toXML(output, "FirstPass");
            if (result && !areEquivalentTestXMLStrings(result, output.str()))
            {
                fputs(output.str(), stdout);
                fflush(stdout);
                throw MakeStringException(100, "Test failed(%s)", "target element");
            }
        }
        catch (IException *E)
        {
            StringBuffer m;
            fprintf(stdout, "\nTest(%s) Exception %d - %s\n", "target element", E->errorCode(), E->errorMessage(m).str());
            E->Release();
            CPPUNIT_ASSERT(false);
        }
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
                              <es:set-value target="Alias" value="'ddd'"/>
                              <es:add-value target="Alias" value="'eee'"/>
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
                              <Alias>ddd</Alias>
                              <Alias>eee</Alias>
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
            runTransform(scriptContext, script, ESDLScriptCtxSection_ESDLRequest, "MyResult", "http post xml", 0);

            StringBuffer output;
            scriptContext->toXML(output, "MyResult");
            if (result && !areEquivalentTestXMLStrings(result, output.str()))
            {
                fputs(output.str(), stdout);
                fflush(stdout);
                throw MakeStringException(100, "Test failed(%s)", "http post xml");
            }
        }
        catch (IException *E)
        {
            StringBuffer m;
            fprintf(stdout, "\nTest(%s) Exception %d - %s\n", "http post xml", E->errorCode(), E->errorMessage(m).str());
            E->Release();
            CPPUNIT_ASSERT(false);
        }
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
      if (result && !areEquivalentTestXMLStrings(result, output.str()))
      {
          fputs(output.str(), stdout);
          fflush(stdout);
          throw MakeStringException(100, "Test failed(%s)", "transform map");
      }
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( ESDLTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( ESDLTests, "ESDL" );

#endif // _USE_CPPUNIT
