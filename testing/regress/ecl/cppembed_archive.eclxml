<Archive build="community_9.14.41-closedown0Debug[heads/GRPC_TEST-0-ga410ec-dirty]"
         eclVersion="9.14.41"
         legacyImport="0"
         legacyWhen="0">
 <Query attributePath="cppembed"/>
 <Option name="eclcc_compiler_version" value="9.14.41"/>
 <Module key="" name="">
  <Attribute key="cppembed"
             name="cppembed"
             sourcePath="/home/some-user/HPCC-Platform/testing/regress/ecl/cppembed.ecl"
             ts="1764669810000000">
   #OPTION(&apos;compileOptions&apos;, &apos;-std=c++17&apos;);

STRING mol() := EMBED(C++)
    #include &quot;cppembed.hpp&quot;

#body
    mol(__lenResult, __result);
ENDEMBED;

mol();&#10;
  </Attribute>
 </Module>
 <AdditionalFiles xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <Manifest isSigned="0" originalFilename="/home/some-user/HPCC-Platform/testing/regress/ecl/cppembed.manifest">
   &lt;Manifest manifestDir=&quot;/home/some-user/HPCC-Platform/testing/regress/ecl/&quot;&gt;
 &lt;Resource filename=&quot;cppembed.cpp&quot;
           originalFilename=&quot;/home/some-user/HPCC-Platform/testing/regress/ecl/cppembed.cpp&quot;
           resourcePath=&quot;/home/some-user/HPCC-Platform/testing/regress/ecl/cppembed.cpp&quot;
           type=&quot;cpp&quot;/&gt;
 &lt;Resource filename=&quot;cppembed.hpp&quot;
           originalFilename=&quot;/home/some-user/HPCC-Platform/testing/regress/ecl/cppembed.hpp&quot;
           resourcePath=&quot;/home/some-user/HPCC-Platform/testing/regress/ecl/cppembed.hpp&quot;
           type=&quot;hpp&quot;/&gt;
&lt;/Manifest&gt;&#10;
  </Manifest>
  <Resource originalFilename="/home/some-user/HPCC-Platform/testing/regress/ecl/cppembed.cpp" resourcePath="/home/some-user/HPCC-Platform/testing/regress/ecl/cppembed.cpp" xsi:type="SOAP-ENC:base64">
   I2luY2x1ZGUgPHN0cmluZz4KCiNpbmNsdWRlICJjcHBlbWJlZC5ocHAiCgp2b2lkIG1vbChz
aXplMzJfdCAmX19sZW5SZXN1bHQsIGNoYXIgKiAmX19yZXN1bHQpCnsKICAgIHN0ZDo6c3Ry
aW5nIG1vbCA9ICJNT0wgaXMgNDIiOwogICAgY29uc3Qgc2l6ZTMyX3QgbGVuID0gc3RhdGlj
X2Nhc3Q8c2l6ZTMyX3Q+KG1vbC5zaXplKCkpOwogICAgY2hhciAqIG91dCA9IChjaGFyKily
dGxNYWxsb2MobGVuKTsKICAgIG1lbWNweShvdXQsIG1vbC5kYXRhKCksIGxlbik7CiAgICBf
X2xlblJlc3VsdCA9IGxlbjsKICAgIF9fcmVzdWx0ID0gb3V0Owp9Cg==  </Resource>
  <Resource originalFilename="/home/some-user/HPCC-Platform/testing/regress/ecl/cppembed.hpp" resourcePath="/home/some-user/HPCC-Platform/testing/regress/ecl/cppembed.hpp" xsi:type="SOAP-ENC:base64">
   I2luY2x1ZGUgImVjbGluY2x1ZGU0LmhwcCIKCnZvaWQgbW9sKHNpemUzMl90ICZfX2xlblJl
c3VsdCwgY2hhciAqICZfX3Jlc3VsdCk7Cg==  </Resource>
 </AdditionalFiles>
</Archive>
