<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:output method="xml" omit-xml-declaration="yes"/>
  <xsl:variable name="logContent" select="/UpdateLogRequest/LogContent"/>
  <xsl:variable name="transactionId" select="$logContent/UserContext/Context/Row/Common/TransactionId"/>
  <xsl:template match="/">
    <Result>
      <Dataset name='all-the-data'>
        <Row>
          <Records>
            <Rec>
              <transaction_id><xsl:value-of select="$transactionId" /></transaction_id>
              <request_data>
                <xsl:text disable-output-escaping="yes">&amp;lt;![CDATA[COMPRESS('</xsl:text>
                <xsl:copy-of select="$logContent"/>
                <xsl:text disable-output-escaping="yes">')]]&amp;gt;</xsl:text>
              </request_data>
              <request_format>SPECIAL</request_format>
              <type>23</type>
            </Rec>
          </Records>
        </Row>
      </Dataset>
    </Result>
  </xsl:template>
</xsl:stylesheet>
