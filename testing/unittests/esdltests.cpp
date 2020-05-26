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
    void runTest(const char *runscript, const char *xml, const char *config, const char *result, int code)
    {
        StringBuffer request(xml);
        Owned<IPropertyTree> cfg = createPTreeFromXMLString(config);
        const char *testname = queryTestName(cfg);

        try
        {
            Owned<IPropertyTree> target = createPTreeFromXMLString(target_config);
            Owned<IPropertyTree> script = createPTreeFromXMLString(runscript);
            Owned<IEsdlCustomTransform> tf = createEsdlCustomTransform(*script, nullptr);

            Owned<IEspContext> ctx = createEspContext(nullptr);//createHttpSecureContext(m_request.get()));
            tf->processTransform(ctx, target, "EsdlExample", "EchoPersonInfo", "EchoPersonInfoRequest", request, cfg);
            if (code)
                throw MakeStringException(99, "Test failed(%s): expected an explicit exception %d", testname, code);
            if (result && !streq(result, request.str()))
            {
                fputs(request.str(), stdout);
                throw MakeStringException(100, "Test failed(%s)", testname);
            }
        }
        catch (IException *E)
        {
            StringBuffer m;
            if (code!=E->errorCode())
            {
                StringBuffer m;
                fprintf(stdout, "\nTest(%s) Expected %d Exception %d - %s\n", testname, code, E->errorCode(), E->errorMessage(m).str());
                throw E;
            }
            E->Release();
        }
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
</soap:Envelope>
)!!";

        runTest(esdlScript, soapRequest, config, result, 0);
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
</soap:Envelope>
)!!";

        runTest(esdlScriptNoPrefix, soapRequest, config, result, 0);
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

        runTest(esdlScript, soapRequest, config, nullptr, 5682);

        constexpr const char *config2 = R"!!(<config strictParams='false'>
  <Transform>
    <Param name='testcase' value="not strict"/>
    <Param name='FailStrict' select='true()'/>
    <Param name='undeclared' select='true()'/>
  </Transform>
</config>)!!";

        runTest(esdlScript, soapRequest, config2, nullptr, 1);
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
</soap:Envelope>
)!!";

        runTest(esdlScript, soapRequest, config, result, 0);
    }

    void testEsdlTransformForEach()
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
          <Relatives>
            <Name>
             <First>Jonathon</First>
             <Alias>John</Alias>
             <Alias>Jon</Alias>
             <Alias>Johnny</Alias>
             <Alias>Johnnie</Alias>
            </Name>
            <Name>
             <First>Jennifer</First>
             <Alias>Jen</Alias>
             <Alias>Jenny</Alias>
             <Alias>Jenna</Alias>
            </Name>
          </Relatives>
        </extra>
        </root>
        )!!";

        static constexpr const char * forEachScript = R"!!(<es:CustomRequestTransform xmlns:es="urn:hpcc:esdl:script" target="extra">
           <es:param name="ForBuildListPath"/>
           <es:param name="ForIdPath"/>
           <es:param name="section"/>
           <es:param name="garbage"/>
           <es:for-each select="$ForBuildListPath">
               <es:variable name="q" select="str:decode-uri('%27')"/>
               <es:variable name="path" select="concat($section, '/Name[First=', $q, First, $q, ']/Aliases', $garbage)"/>
               <es:for-each select="Alias">
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

        constexpr const char * resultFriends = R"!!(<root>
 <extra>
  <Relatives>
   <Name>
    <Alias>John</Alias>
    <Alias>Jon</Alias>
    <Alias>Johnny</Alias>
    <Alias>Johnnie</Alias>
    <First>Jonathon</First>
   </Name>
   <Name>
    <Alias>Jen</Alias>
    <Alias>Jenny</Alias>
    <Alias>Jenna</Alias>
    <First>Jennifer</First>
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
 </extra>
</root>
)!!";

        constexpr const char *configFriends = R"!!(<config strictParams="true">
  <Transform>
    <Param name='testcase' value="for each friend"/>
    <Param name='section' select="'Friends'"/>
    <Param name='garbage' select="''"/>
    <Param name='ForBuildListPath' select='/root/extra/Friends/Name'/>
    <Param name='ForIdPath' select='extra/Friends/Name/First'/>
  </Transform>
</config>)!!";

        runTest(forEachScript, input, configFriends, resultFriends, 0);

        constexpr const char * resultRelatives = R"!!(<root>
 <extra>
  <Relatives>
   <Name>
    <Alias>John</Alias>
    <Alias>Jon</Alias>
    <Alias>Johnny</Alias>
    <Alias>Johnnie</Alias>
    <Aliases>John,Jon,Johnny,Johnnie</Aliases>
    <First>Jonathon</First>
   </Name>
   <Name>
    <Alias>Jen</Alias>
    <Alias>Jenny</Alias>
    <Alias>Jenna</Alias>
    <Aliases>Jen,Jenny,Jenna</Aliases>
    <First>Jennifer</First>
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
    <Alias>Moe</Alias>
    <Alias>Poe</Alias>
    <Alias>Doe</Alias>
    <First>Joe</First>
   </Name>
   <Name>
    <Alias>Jan</Alias>
    <Alias>Janie</Alias>
    <Alias>Janet</Alias>
    <First>Jane</First>
   </Name>
  </Friends>
 </extra>
</root>
)!!";

        constexpr const char *configRelatives = R"!!(<config strictParams="true">
  <Transform>
    <Param name='testcase' value="for each relative"/>
    <Param name='section' select="'Relatives'"/>
    <Param name='garbage' select="''"/>
    <Param name='ForBuildListPath' select='extra/Relatives/Name'/>
    <Param name='ForIdPath' select='/root/extra/Relatives/Name/First'/>
  </Transform>
</config>)!!";

        runTest(forEachScript, input, configRelatives, resultRelatives, 0);

        constexpr const char *configGarbagePathError = R"!!(<config strictParams="true">
  <Transform>
    <Param name='testcase' value="for each garbage path error"/>
    <Param name='section' select="'Friends'"/>
    <Param name='garbage' select="'##'"/>
    <Param name='ForBuildListPath' select='extra/Friends/Name'/>
    <Param name='ForIdPath' select='extra/Friends/Name/First'/>
  </Transform>
</config>)!!";

        runTest(forEachScript, input, configGarbagePathError, nullptr, -1);

        constexpr const char * resultNada = R"!!(<root>
 <extra>
  <Relatives>
   <Name>
    <Alias>John</Alias>
    <Alias>Jon</Alias>
    <Alias>Johnny</Alias>
    <Alias>Johnnie</Alias>
    <First>Jonathon</First>
   </Name>
   <Name>
    <Alias>Jen</Alias>
    <Alias>Jenny</Alias>
    <Alias>Jenna</Alias>
    <First>Jennifer</First>
   </Name>
  </Relatives>
  <Friends>
   <Name>
    <Alias>Moe</Alias>
    <Alias>Poe</Alias>
    <Alias>Doe</Alias>
    <First>Joe</First>
   </Name>
   <Name>
    <Alias>Jan</Alias>
    <Alias>Janie</Alias>
    <Alias>Janet</Alias>
    <First>Jane</First>
   </Name>
  </Friends>
 </extra>
</root>
)!!";

        constexpr const char *configNada = R"!!(<config strictParams="true">
  <Transform>
    <Param name='testcase' value="for each nada target"/>
    <Param name='section' select="'Friends'"/>
    <Param name='garbage' select="'/nada'"/>
    <Param name='ForBuildListPath' select='extra/Nada/Name'/>
    <Param name='ForIdPath' select='extra/Friends/Name/First/Nada'/>
  </Transform>
</config>)!!";

        runTest(forEachScript, input, configNada, resultNada, 0);
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
           <for-each select="/root/extra/Friends/Name">
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
</root>
)!!";

        constexpr const char *config = R"!!(<config strictParams="true">
  <Transform>
    <Param name='testcase' value="variable scope"/>
  </Transform>
</config>)!!";

        runTest(script, input, config, result, 0);
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
            <es:SetValue target="ID" select="/root/Person/Name/First"/>
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
</root>
)!!";

        runTest(script, input, config, result, 0);
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
</root>
)!!";

        runTest(script, input, config, result, 0);
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

        runTest(script, input, config, nullptr, -1); //createPropBranch: cannot create path : ID##

        static constexpr const char * script2 = R"!!(<es:CustomRequestTransform xmlns:es="urn:hpcc:esdl:script" target="Person">
            <es:SetValue target="ID##" select="/root/Person/Name/First"/>
            <es:AppendValue target="Append" select="Person/Name/First"/>
            <es:AppendValue target="Append" value="'++'"/>
        </es:CustomRequestTransform>
        )!!";

        runTest(script2, input, config, nullptr, -1); //createPropBranch: cannot create path : ID##
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

        runTest(esdlScript, soapRequest, config, nullptr, 11);
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

        runTest(esdlScript, soapRequest, config, nullptr, 12);
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

        runTest(esdlScript, soapRequest, config, nullptr, 13);
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

        runTest(esdlScript, soapRequest, config, nullptr, 21);
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

        runTest(esdlScript, soapRequest, config, nullptr, 22);
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

        runTest(esdlScript, soapRequest, config, nullptr, 23);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( ESDLTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( ESDLTests, "ESDL" );

#endif // _USE_CPPUNIT
