<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:output method="xml" omit-xml-declaration="yes"/>
  <xsl:template match="/">
    <Result>
      <Dataset name='include-cdata'>
        <Record>
          <![CDATA[<some><arbitraryxml>testdata</arbitraryxml></some>]]>
          <Carrot action="chop">
            <![CDATA[
              <![CDATA[
              <AlreadyNestedButCorrectlyEncodedNode encodedEndMarker="]]]]><![CDATA[>"/>
              ]]]]><![CDATA[>
            ]]>
            <Celery rawEndMarker="]]>"/>
          </Carrot>
        </Record>
      </Dataset>
    </Result>
  </xsl:template>
</xsl:stylesheet>