<Archive build="internal_6.4.8-rc1Debug"
         eclVersion="6.4.8"
         legacyImport="0"
         legacyWhen="0">
 <Query attributePath="TestX.Fred.Sue.Jo.BWR_Test"/>
 <Module key="testx" name="TestX"/>
 <Module key="testx.fred" name="TestX.Fred">
  <Attribute key="testme" name="TestMe" sourcePath="/home/gavin/dev/issue18657/./TestX/Fred/TestMe.ecl">
   export TestMe := 10;&#10;&#10;
  </Attribute>
 </Module>
 <Module key="testx.fred.sue" name="TestX.Fred.Sue"/>
 <Module key="testx.fred.sue.jo" name="TestX.Fred.Sue.Jo">
  <Attribute key="bwr_test" name="BWR_Test" sourcePath="TestX/Fred/Sue/Jo/BWR_Test.ecl">
   IMPORT $.^.^ AS M1;
OUTPUT(M1.TestMe);&#10;
  </Attribute>
 </Module>
</Archive>
