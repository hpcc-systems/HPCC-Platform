<Archive build="community_9.12.11-closedown0Debug"
         eclVersion="1.1.1"
         legacyImport="1"
         legacyWhen="1">
 <Query originalFilename="/home/gavin/dev/hpcc/ecl/regress/temp.ecl">
  import Std as *;

doMyService := FUNCTION
  O := OUTPUT(&apos;Did a Service for: &apos; + &apos;EVENTNAME=&apos; + EVENTNAME);
  N := NOTIFY(EVENT(&apos;MyServiceComplete&apos;,
                    &apos;&amp;lt;Event&amp;gt;&amp;lt;returnTo&amp;gt;FRED&amp;lt;/returnTo&amp;gt;&amp;lt;/Event&amp;gt;&apos;),
                    EVENTEXTRA(&apos;returnTo&apos;));
  RETURN WHEN(EVENTEXTRA(&apos;returnTo&apos;),ORDERED(O,N));
END;
OUTPUT(doMyService) : WHEN(&apos;MyService&apos;);

// and a call (in a separate workunit):
NOTIFY(&apos;MyService&apos;,
       &apos;&amp;lt;Event&amp;gt;&amp;lt;returnTo&amp;gt;&apos;+ WORKUNIT + &apos;&amp;lt;/returnTo&amp;gt;&amp;lt;/Event&amp;gt;&apos;);
WAIT(&apos;MyServiceComplete&apos;);
OUTPUT(&apos;WORKUNIT DONE&apos;)&#10;
 </Query>
 <OnWarning value="3118=ignore"/>
 <Module key="_versions" name="_versions"/>
 <Module key="std" name="std"/>
 <Module key="" name="">
  <Attribute key="supportvectormachines"
             name="SupportVectorMachines"
             sourcePath="/home/gavin/.HPCCSystems/bundles/SupportVectorMachines.ecl"
             ts="1673970132000000">
   IMPORT _versions.SupportVectorMachines.V1_1.SupportVectorMachines as _SupportVectorMachines; EXPORT SupportVectorMachines := _SupportVectorMachines;
  </Attribute>
  <Attribute key="textvectors"
             name="TextVectors"
             sourcePath="/home/gavin/.HPCCSystems/bundles/TextVectors.ecl"
             ts="1712654230000000">
   IMPORT _versions.TextVectors.V1_0_1.TextVectors as _TextVectors; EXPORT TextVectors := _TextVectors;
  </Attribute>
 </Module>
 <Module key="_versions.supportvectormachines" name="_versions.SupportVectorMachines"/>
 <Module key="_versions.supportvectormachines.v1_1" name="_versions.SupportVectorMachines.V1_1"/>
 <Module key="_versions.supportvectormachines.v1_1.supportvectormachines" name="_versions.SupportVectorMachines.V1_1.SupportVectorMachines"/>
 <Module key="_versions.textvectors" name="_versions.TextVectors"/>
 <Module key="_versions.textvectors.v1_0_1" name="_versions.TextVectors.V1_0_1"/>
 <Module key="_versions.textvectors.v1_0_1.textvectors" name="_versions.TextVectors.V1_0_1.TextVectors"/>
 <Option name="spanmultiplecpp" value="0"/>
 <Option name="savecpptempfiles" value="1"/>
 <Option name="eclcc_compiler_version" value="9.12.11"/>
 <Option name="debugquery" value="1"/>
</Archive>
