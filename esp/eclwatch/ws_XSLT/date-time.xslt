<?xml version="1.0" encoding="UTF-8"?>
<!--

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
-->

<xsl:stylesheet
  version="1.0"
   xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:date="http://www.ora.com/XSLTCookbook/namespaces/date" extension-element-prefixes="date" id="date:date-time">

<xsl:output method="xml" indent="yes"/>

<xsl:template name="date:format-date-time">
    <xsl:param name="year"/>
    <xsl:param name="month"/>
    <xsl:param name="day"/>
    <xsl:param name="hour"/>
    <xsl:param name="minute"/>
    <xsl:param name="second"/>
    <xsl:param name="time-zone"/>
    <xsl:param name="format" select="'%Y-%m-%dT%H:%M:%S%z'"/>

    <xsl:choose>
      <xsl:when test="contains($format, '%')">
        <xsl:value-of select="substring-before($format, '%')"/>
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="$format"/>
      </xsl:otherwise>
    </xsl:choose>

    <xsl:variable name="code"
                  select="substring(substring-after($format, '%'), 1, 1)"/>
    <xsl:choose>

      <!-- Abbreviated weekday name -->
      <xsl:when test="$code='a'">
        <xsl:variable name="day-of-the-week">
          <xsl:call-template name="date:calculate-day-of-the-week">
            <xsl:with-param name="year" select="$year"/>
            <xsl:with-param name="month" select="$month"/>
            <xsl:with-param name="day" select="$day"/>
          </xsl:call-template>
        </xsl:variable>
        <xsl:call-template name="date:get-day-of-the-week-abbreviation">
          <xsl:with-param name="day-of-the-week" 
             select="$day-of-the-week"/>
        </xsl:call-template>
      </xsl:when>

      <!-- Full weekday name -->
      <xsl:when test="$code='A'">
        <xsl:variable name="day-of-the-week">
          <xsl:call-template name="date:calculate-day-of-the-week">
            <xsl:with-param name="year" select="$year"/>
            <xsl:with-param name="month" select="$month"/>
            <xsl:with-param name="day" select="$day"/>
          </xsl:call-template>
        </xsl:variable>
        <xsl:call-template name="date:get-day-of-the-week-name">
          <xsl:with-param name="day-of-the-week" 
                          select="$day-of-the-week"/>
        </xsl:call-template>
      </xsl:when>

      <!-- Abbreviated month name -->
      <xsl:when test="$code='b'">
        <xsl:call-template name="date:get-month-abbreviation">
          <xsl:with-param name="month" select="$month"/>
        </xsl:call-template>
      </xsl:when>

      <!-- Full month name -->
      <xsl:when test="$code='B'">
        <xsl:call-template name="date:get-month-name">
          <xsl:with-param name="month" select="$month"/>
        </xsl:call-template>
      </xsl:when>

      <!-- Date and time representation appropriate for locale -->
      <xsl:when test="$code='c'">
        <xsl:text>[not implemented]</xsl:text>
      </xsl:when>

      <!-- Day of month as decimal number (01 - 31) -->
      <xsl:when test="$code='d'">
        <xsl:value-of select="concat(substring('00',1,2 - string-length($day)),$day)"/>
      </xsl:when>

      <!-- Hour in 24-hour format (00 - 23) -->
      <xsl:when test="$code='H'">
        <xsl:value-of select="format-number($hour,'00')"/>
      </xsl:when>

      <!-- Hour in 12-hour format (01 - 12) -->
      <xsl:when test="$code='I'">
        <xsl:choose>
          <xsl:when test="$hour = 0">12</xsl:when>
          <xsl:when test="$hour &lt; 13">
            <xsl:value-of select="format-number($hour,'00')"/>
          </xsl:when>
          <xsl:otherwise>
            <xsl:value-of select="format-number($hour - 12,'00')"/>
         </xsl:otherwise>
        </xsl:choose>
      </xsl:when>

      <!-- Day of year as decimal number (001 - 366) -->
      <xsl:when test="$code='j'">
        <xsl:variable name="diff"> 
         <xsl:call-template name="date:date-difference">
           <xsl:with-param name="from-year" select="$year"/>
           <xsl:with-param name="from-month" select="1"/>
           <xsl:with-param name="form-day" select="1"/>
           <xsl:with-param name="to-year" select="$year"/>
           <xsl:with-param name="to-month" select="$month"/>
           <xsl:with-param name="to-day" select="$day"/>
         </xsl:call-template>
        </xsl:variable> 
        <xsl:value-of select="format-number($diff + 1, '000')"/>
      </xsl:when>

      <!-- Month as decimal number (01 - 12) -->
      <xsl:when test="$code='m'">
        <xsl:value-of select="concat(substring('00',1,2 - string-length($month)),$month)"/>
      </xsl:when>

      <!-- Minute as decimal number (00 - 59) -->
      <xsl:when test="$code='M'">
        <xsl:value-of select="format-number($minute,'00')"/>
      </xsl:when>

      <!-- Current locale's A.M./P.M. indicator for 12-hour clock -->
      <xsl:when test="$code='p'">
        <xsl:choose>
          <xsl:when test="$hour &lt; 12">AM</xsl:when>
          <xsl:otherwise>PM</xsl:otherwise>
        </xsl:choose>
      </xsl:when>

      <!-- Second as decimal number (00 - 59) -->
      <xsl:when test="$code='S'">
        <xsl:value-of select="format-number($second,'00')"/>
      </xsl:when>

      <!-- Week of year as decimal number, 
           with Sunday as first day of week (00 - 53) -->
      <xsl:when test="$code='U'">
        <!-- add 1 to day -->
        <xsl:call-template name="date:calculate-week-number">
          <xsl:with-param name="year" select="$year"/>
          <xsl:with-param name="month" select="$month"/>
          <xsl:with-param name="day" select="$day + 1"/>
        </xsl:call-template>
      </xsl:when>

      <!-- Weekday as decimal number (0 - 6; Sunday is 0) -->
      <xsl:when test="$code='w'">
        <xsl:call-template name="date:calculate-day-of-the-week">
          <xsl:with-param name="year" select="$year"/>
          <xsl:with-param name="month" select="$month"/>
          <xsl:with-param name="day" select="$day"/>
        </xsl:call-template>
      </xsl:when>

      <!-- Week of year as decimal number, 
           with Monday as first day of week (00 - 53) -->
      <xsl:when test="$code='W'">
        <xsl:call-template name="date:calculate-week-number">
          <xsl:with-param name="year" select="$year"/>
          <xsl:with-param name="month" select="$month"/>
          <xsl:with-param name="day" select="$day"/>
        </xsl:call-template>
      </xsl:when>

      <!-- Date representation for current locale -->
      <xsl:when test="$code='x'">
        <xsl:text>[not implemented]</xsl:text>
      </xsl:when>

      <!-- Time representation for current locale -->
      <xsl:when test="$code='X'">
        <xsl:text>[not implemented]</xsl:text>
      </xsl:when>

      <!-- Year without century, as decimal number (00 - 99) -->
      <xsl:when test="$code='y'">
        <xsl:value-of select="format-number($year mod 100,'00')"/>  
      </xsl:when>

      <!-- Year with century, as decimal number -->
      <xsl:when test="$code='Y'">
        <xsl:value-of select="format-number($year,'0000')"/>
      </xsl:when>

      <!-- Time-zone name or abbreviation; -->
      <!-- no characters if time zone is unknown -->
      <xsl:when test="$code='z'">
        <xsl:value-of select="$time-zone"/>
      </xsl:when>

      <!-- Percent sign -->
      <xsl:when test="$code='%'">
        <xsl:text>%</xsl:text>
      </xsl:when>

    </xsl:choose>

    <xsl:variable name="remainder" 
                  select="substring(substring-after($format, '%'), 2)"/>

    <xsl:if test="$remainder">
      <xsl:call-template name="date:format-date-time">
        <xsl:with-param name="year" select="$year"/>
        <xsl:with-param name="month" select="$month"/>
        <xsl:with-param name="day" select="$day"/>
        <xsl:with-param name="hour" select="$hour"/>
        <xsl:with-param name="minute" select="$minute"/>
        <xsl:with-param name="second" select="$second"/>
        <xsl:with-param name="time-zone" select="$time-zone"/>
        <xsl:with-param name="format" select="$remainder"/>
      </xsl:call-template>
    </xsl:if>

</xsl:template>


  <xsl:template name="date:calculate-day-of-the-week">
    <xsl:param name="date-time"/>
    <xsl:param name="date" select="substring-before($date-time,'T')"/>
    <xsl:param name="year" select="substring-before($date,'-')"/>
    <xsl:param name="month" select="substring-before(substring-after($date,'-'),'-')"/>
    <xsl:param name="day" select="substring-after(substring-after($date,'-'),'-')"/>
    
    <xsl:variable name="a" select="floor((14 - $month) div 12)"/>
    <xsl:variable name="y" select="$year - $a"/>
    <xsl:variable name="m" select="$month + 12 * $a - 2"/>

    <xsl:value-of select="($day + $y + floor($y div 4) - floor($y div 100) + floor($y div 400) + floor((31 * $m) div 12)) mod 7"/>

  </xsl:template>

  <xsl:template name="date:get-day-of-the-week-name">
    <xsl:param name="day-of-the-week"/>

    <xsl:choose>
      <xsl:when test="$day-of-the-week = 0">Sunday</xsl:when>
      <xsl:when test="$day-of-the-week = 1">Monday</xsl:when>
      <xsl:when test="$day-of-the-week = 2">Tuesday</xsl:when>
      <xsl:when test="$day-of-the-week = 3">Wednesday</xsl:when>
      <xsl:when test="$day-of-the-week = 4">Thursday</xsl:when>
      <xsl:when test="$day-of-the-week = 5">Friday</xsl:when>
      <xsl:when test="$day-of-the-week = 6">Saturday</xsl:when>
      <xsl:otherwise>error: <xsl:value-of select="$day-of-the-week"/></xsl:otherwise>
    </xsl:choose>

  </xsl:template>

  <xsl:template name="date:get-day-of-the-week-abbreviation">
    <xsl:param name="day-of-the-week"/>

    <xsl:choose>
      <xsl:when test="$day-of-the-week = 0">Sun</xsl:when>
      <xsl:when test="$day-of-the-week = 1">Mon</xsl:when>
      <xsl:when test="$day-of-the-week = 2">Tue</xsl:when>
      <xsl:when test="$day-of-the-week = 3">Wed</xsl:when>
      <xsl:when test="$day-of-the-week = 4">Thu</xsl:when>
      <xsl:when test="$day-of-the-week = 5">Fri</xsl:when>
      <xsl:when test="$day-of-the-week = 6">Sat</xsl:when>
      <xsl:otherwise>error: <xsl:value-of select="$day-of-the-week"/></xsl:otherwise>
    </xsl:choose>

  </xsl:template>

  <xsl:template name="date:get-month-name">
    <xsl:param name="date-time"/>
    <xsl:param name="date" select="substring-before($date-time,'T')"/>
    <xsl:param name="month" select="substring-before(substring-after($date,'-'),'-')"/>
    
    <xsl:choose>
      <xsl:when test="$month = 1">January</xsl:when>
      <xsl:when test="$month = 2">February</xsl:when>
      <xsl:when test="$month = 3">March</xsl:when>
      <xsl:when test="$month = 4">April</xsl:when>
      <xsl:when test="$month = 5">May</xsl:when>
      <xsl:when test="$month = 6">June</xsl:when>
      <xsl:when test="$month = 7">July</xsl:when>
      <xsl:when test="$month = 8">August</xsl:when>
      <xsl:when test="$month = 9">September</xsl:when>
      <xsl:when test="$month = 10">October</xsl:when>
      <xsl:when test="$month = 11">November</xsl:when>
      <xsl:when test="$month = 12">December</xsl:when>
      <xsl:otherwise>error: <xsl:value-of select="$month"/></xsl:otherwise>
    </xsl:choose>

  </xsl:template>

  <xsl:template name="date:get-month-abbreviation">
    <xsl:param name="date-time"/>
    <xsl:param name="date" select="substring-before($date-time,'T')"/>
    <xsl:param name="month" select="substring-before(substring-after($date,'-'),'-')"/>
    
    <xsl:choose>
      <xsl:when test="$month = 1">Jan</xsl:when>
      <xsl:when test="$month = 2">Feb</xsl:when>
      <xsl:when test="$month = 3">Mar</xsl:when>
      <xsl:when test="$month = 4">Apr</xsl:when>
      <xsl:when test="$month = 5">May</xsl:when>
      <xsl:when test="$month = 6">Jun</xsl:when>
      <xsl:when test="$month = 7">Jul</xsl:when>
      <xsl:when test="$month = 8">Aug</xsl:when>
      <xsl:when test="$month = 9">Sep</xsl:when>
      <xsl:when test="$month = 10">Oct</xsl:when>
      <xsl:when test="$month = 11">Nov</xsl:when>
      <xsl:when test="$month = 12">Dec</xsl:when>
      <xsl:otherwise>error: <xsl:value-of select="$month"/></xsl:otherwise>
    </xsl:choose>

  </xsl:template>

  <xsl:template name="date:date-to-julian-day">
    <xsl:param name="date-time"/>
    <xsl:param name="date" select="substring-before($date-time,'T')"/>
    <xsl:param name="year" select="substring-before($date,'-')"/>
    <xsl:param name="month" select="substring-before(substring-after($date,'-'),'-')"/>
    <xsl:param name="day" select="substring-after(substring-after($date,'-'),'-')"/>
    
    <xsl:variable name="a" select="floor((14 - $month) div 12)"/>
    <xsl:variable name="y" select="$year + 4800 - $a"/>
    <xsl:variable name="m" select="$month + 12 * $a - 3"/>

    <xsl:value-of select="$day + floor((153 * $m + 2) div 5) + 365 * $y + floor($y div 4) - floor($y div 100) + floor($y div 400) - 32045"/>

  </xsl:template>

  <xsl:template name="date:julian-date-to-julian-day">
    <xsl:param name="date-time"/>
    <xsl:param name="date" select="substring-before($date-time,'T')"/>
    <xsl:param name="year" select="substring-before($date,'-')"/>
    <xsl:param name="month" select="substring-before(substring-after($date,'-'),'-')"/>
    <xsl:param name="day" select="substring-after(substring-after($date,'-'),'-')"/>
    
    <xsl:variable name="a" select="floor((14 - $month) div 12)"/>
    <xsl:variable name="y" select="$year + 4800 - $a"/>
    <xsl:variable name="m" select="$month + 12 * $a - 3"/>

    <xsl:value-of select="$day + floor((153 * $m + 2) div 5) + 365 * $y + floor($y div 4) - 32083"/>

  </xsl:template>


  <xsl:template name="date:format-julian-day">
    <xsl:param name="julian-day"/>
    <xsl:param name="format" select="'%Y-%m-%d'"/>

    <xsl:variable name="a" select="$julian-day + 32044"/>
    <xsl:variable name="b" select="floor((4 * $a + 3) div 146097)"/>
    <xsl:variable name="c" select="$a - floor(($b * 146097) div 4)"/>

    <xsl:variable name="d" select="floor((4 * $c + 3) div 1461)"/>
    <xsl:variable name="e" select="$c - floor((1461 * $d) div 4)"/>
    <xsl:variable name="m" select="floor((5 * $e + 2) div 153)"/>

    <xsl:variable name="day" select="$e - floor((153 * $m + 2) div 5) + 1"/>
    <xsl:variable name="month" select="$m + 3 - 12 * floor($m div 10)"/>
    <xsl:variable name="year" select="$b * 100 + $d - 4800 + floor($m div 10)"/>

    <xsl:call-template name="date:format-date-time">
      <xsl:with-param name="year" select="$year"/>
      <xsl:with-param name="month" select="$month"/>
      <xsl:with-param name="day" select="$day"/>
      <xsl:with-param name="format" select="$format"/>
    </xsl:call-template>

  </xsl:template>

  <xsl:template name="date:calculate-week-number">
    <xsl:param name="year"/>
    <xsl:param name="month"/>
    <xsl:param name="day"/>

    <xsl:variable name="J">
      <xsl:call-template name="date:date-to-julian-day">
        <xsl:with-param name="year" select="$year"/>
        <xsl:with-param name="month" select="$month"/>
        <xsl:with-param name="day" select="$day"/>
      </xsl:call-template>
    </xsl:variable>

    <xsl:variable name="d4" select="($J + 31741 - ($J mod 7)) mod 146097 mod 36524 mod 1461"/>
    <xsl:variable name="L" select="floor($d4 div 1460)"/>
    <xsl:variable name="d1" select="(($d4 - $L) mod 365) + $L"/>

    <xsl:value-of select="floor($d1 div 7) + 1"/>

  </xsl:template>

<!-- These are adapted from Rheingold, et. al. -->

<xsl:template name="date:last-day-of-month">
    <xsl:param name="date-time"/>
    <xsl:param name="date" select="substring-before($date-time,'T')"/>
    <xsl:param name="year" select="substring-before($date,'-')"/>
    <xsl:param name="month" select="substring-before(substring-after($date,'-'),'-')"/>
    
    <xsl:choose>
        <xsl:when test="$month = 2 and not($year mod 4) and ($year mod 100 or not($year mod 400))">
            <xsl:value-of select="29"/>
        </xsl:when>
        <xsl:otherwise>
            <xsl:value-of select="substring('312831303130313130313031',2 * $month - 1,2)"/>
        </xsl:otherwise>
    </xsl:choose>
</xsl:template>

<xsl:template name="date:julian-day-to-absolute-day">
    <xsl:param name="j-day"/>
    <xsl:value-of select="$j-day - 1721425"/>
</xsl:template>

<xsl:template name="date:absolute-day-to-julian-day">
    <xsl:param name="abs-day"/>
    <xsl:value-of select="$abs-day + 1721425"/>
</xsl:template>

<xsl:template name="date:date-to-absolute-day">
    <xsl:param name="date-time"/>
    <xsl:param name="date" select="substring-before($date-time,'T')"/>
    <xsl:param name="year" select="substring-before($date,'-')"/>
    <xsl:param name="month" select="substring-before(substring-after($date,'-'),'-')"/>
    <xsl:param name="day" select="substring-after(substring-after($date,'-'),'-')"/>
    
    <xsl:call-template name="date:julian-day-to-absolute-day">
      <xsl:with-param name="j-day">
        <xsl:call-template name="date:date-to-julian-day">
          <xsl:with-param name="year" select="$year"/>
          <xsl:with-param name="month" select="$month"/>
          <xsl:with-param name="day" select="$day"/>
        </xsl:call-template>
      </xsl:with-param>
  </xsl:call-template>
</xsl:template>

<xsl:template name="date:absolute-day-to-date">
  <xsl:param name="abs-day"/>
  
  <xsl:call-template name="date:julian-day-to-date">
    <xsl:with-param name="j-day">
        <xsl:call-template name="date:absolute-day-to-julian-day">
            <xsl:with-param name="abs-day" select="$abs-day"/>
        </xsl:call-template>
    </xsl:with-param>
  </xsl:call-template>
</xsl:template>

<xsl:template name="date:k-day-on-or-before-abs-day">
    <xsl:param name="abs-day"/>
    <xsl:param name="k"/>
    <xsl:value-of select="$abs-day - (($abs-day - $k) mod 7)"/>
</xsl:template>

<xsl:template name="date:iso-date-to-absolute-day">
    <xsl:param name="iso-week"/>
    <xsl:param name="iso-day"/>
    <xsl:param name="iso-year"/>
    
    <xsl:variable name="a">
        <xsl:call-template name="date:date-to-absolute-day">
            <xsl:with-param name="year" select="$iso-year"/>
            <xsl:with-param name="month" select="1"/>
            <xsl:with-param name="day" select="4"/>
        </xsl:call-template>
    </xsl:variable>

    <xsl:variable name="days-in-prior-yrs">
        <xsl:call-template name="date:k-day-on-or-before-abs-day">
            <xsl:with-param name="abs-day" select="$a"/>
            <xsl:with-param name="k" select="1"/>
        </xsl:call-template>
    </xsl:variable>

    <xsl:variable name="days-in-prior-weeks-this-yr"    select="7 * ($iso-week - 1)"/>

    <xsl:variable name="prior-days-this-week"   select="$iso-day - 1"/>

    <xsl:value-of select="$days-in-prior-yrs + $days-in-prior-weeks-this-yr + $prior-days-this-week"/>  
</xsl:template>


<xsl:template name="date:absolute-day-to-iso-date">
    <xsl:param name="abs-day"/>
    
    <xsl:variable name="d">
        <xsl:call-template name="date:absolute-day-to-date">
            <xsl:with-param name="abs-day" select="$abs-day - 3"/>
        </xsl:call-template>
    </xsl:variable>
    
    <xsl:variable name="approx" select="substring-before($d,'/')"/>
    
    <xsl:variable name="iso-year">
        <xsl:variable name="a">
            <xsl:call-template name="date:iso-date-to-absolute-day">
                <xsl:with-param name="iso-week" select="1"/>
                <xsl:with-param name="iso-day" select="1"/>
                <xsl:with-param name="iso-year" select="$approx + 1"/>
            </xsl:call-template>
        </xsl:variable>
        <xsl:choose>
            <xsl:when test="$abs-day >= $a">
                <xsl:value-of select="$approx + 1"/>
            </xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="$approx"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:variable>

    <xsl:variable name="iso-week">
        <xsl:variable name="a">
            <xsl:call-template name="date:iso-date-to-absolute-day">
                <xsl:with-param name="iso-week" select="1"/>
                <xsl:with-param name="iso-day" select="1"/>
                <xsl:with-param name="iso-year" select="$iso-year"/>
            </xsl:call-template>
        </xsl:variable>
        <xsl:value-of select="1 + floor(($abs-day - $a) div 7)"/>
    </xsl:variable>
    
    <xsl:variable name="iso-day">
        <xsl:variable name="a" select="$abs-day mod 7"/>
        <xsl:choose>
            <xsl:when test="not($a)">
                <xsl:value-of select="7"/>
            </xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="$a"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:variable>

    <xsl:value-of select="concat($iso-year,'/W',$iso-week,'/',$iso-day)"/>
        
</xsl:template>

<xsl:template name="date:last-day-of-julian-month">
    <xsl:param name="month"/>
    <xsl:param name="year"/>
    <xsl:choose>
        <xsl:when test="$month = 2 and not($year mod 4)">
            <xsl:value-of select="29"/>
        </xsl:when>
        <xsl:otherwise>
            <xsl:value-of select="substring('312831303130313130313031',2 * $month - 1,2)"/>
        </xsl:otherwise>
    </xsl:choose>
</xsl:template>

<xsl:template name="date:julian-date-to-absolute-day">
    <xsl:param name="year"/>
    <xsl:param name="month"/>
    <xsl:param name="day"/>
    
    <xsl:call-template name="date:julian-day-to-absolute-day">
        <xsl:with-param name="j-day">
            <xsl:call-template name="date:julian-date-to-julian-day">
                <xsl:with-param name="year" select="$year"/>
                <xsl:with-param name="month" select="$month"/>
                <xsl:with-param name="day" select="$day"/>
            </xsl:call-template>
        </xsl:with-param>
    </xsl:call-template>
</xsl:template>

<xsl:template name="date:julian-day-to-julian-date">
    <xsl:param name="j-day"/>

    <xsl:call-template name="date:julian-or-gregorian-date-elem">
        <xsl:with-param name="b" select="0"/>
        <xsl:with-param name="c" select="$j-day + 32082"/>
    </xsl:call-template>

</xsl:template>

<xsl:template name="date:julian-day-to-date">
    <xsl:param name="j-day"/>

    <xsl:variable name="a" select="$j-day + 32044"/>
    <xsl:variable name="b" select="floor((4 * $a + 3) div 146097)"/>
    <xsl:variable name="c" select="$a - floor((146097 * $b) div 4)"/>

    <xsl:call-template name="date:julian-or-gregorian-date-elem">
        <xsl:with-param name="b" select="$b"/>
        <xsl:with-param name="c" select="$c"/>
    </xsl:call-template>
</xsl:template>


<xsl:template name="date:julian-or-gregorian-date-elem">
    <xsl:param name="b"/>
    <xsl:param name="c"/>

    <xsl:variable name="d" select="floor((4 * $c + 3) div 1461)"/>
    <xsl:variable name="e" select="$c - floor((1461 * $d) div 4)"/>
    <xsl:variable name="m" select="floor((5 * $e + 2) div 153)"/>

    <xsl:variable name="day" 
        select="$e - floor((153 * $m + 2) div 5) + 1"/>

    <xsl:variable name="month" 
        select="$m + 3 - (12 * floor($m div 10))"/>

    <xsl:variable name="year" 
        select="100 * $b + $d - 4800 + floor($m div 10)"/>

    <xsl:value-of select="concat($year,'/',$month,'/',$day)"/>
    
</xsl:template>



<!-- Holidays -->

<xsl:template name="date:n-th-k-day">
    <xsl:param name="n"/> <!-- Postive n counts from beginning of month; negative from end. -->
    <xsl:param name="k"/>
    <xsl:param name="month"/>
    <xsl:param name="year"/>

    <xsl:choose>
        <xsl:when test="$n > 0">
            <xsl:variable name="k-day-on-or-before">
                <xsl:variable name="abs-day">
                    <xsl:call-template name="date:date-to-absolute-day">
                        <xsl:with-param name="month" select="$month"/>
                        <xsl:with-param name="day" select="7"/>
                        <xsl:with-param name="year" select="$year"/>
                    </xsl:call-template>
                </xsl:variable>
                <xsl:call-template name="date:k-day-on-or-before-abs-day">
                    <xsl:with-param name="abs-day" select="$abs-day"/>
                    <xsl:with-param name="k" select="$k"/>
                </xsl:call-template>
            </xsl:variable>
            <xsl:value-of select="$k-day-on-or-before + 7 * ($n - 1)"/>
        </xsl:when>
        <xsl:otherwise>
            <xsl:variable name="k-day-on-or-before">
                <xsl:variable name="abs-day">
                    <xsl:call-template name="date:date-to-absolute-day">
                        <xsl:with-param name="month" select="$month"/>
                        <xsl:with-param name="day">
                            <xsl:call-template name="date:last-day-of-month">
                                <xsl:with-param name="month" select="$month"/>
                                <xsl:with-param name="year" select="$year"/>
                            </xsl:call-template>
                        </xsl:with-param>
                        <xsl:with-param name="year" select="$year"/>
                    </xsl:call-template>
                </xsl:variable>
                <xsl:call-template name="date:k-day-on-or-before-abs-day">
                    <xsl:with-param name="abs-day" select="$abs-day"/>
                    <xsl:with-param name="k" select="$k"/>
                </xsl:call-template>
            </xsl:variable>
            <xsl:value-of select="$k-day-on-or-before + 7 * ($n + 1)"/>
        </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

<xsl:template name="date:calculate-julian-day">
    <xsl:param name="year"/>
    <xsl:param name="month"/>
    <xsl:param name="day"/>

    <xsl:variable name="a" select="floor((14 - $month) div 12)"/>
    <xsl:variable name="y" select="$year + 4800 - $a"/>
    <xsl:variable name="m" select="$month + 12 * $a - 3"/>

    <xsl:value-of select="$day + floor((153 * $m + 2) div 5) + $y * 365 + 
        floor($y div 4) - floor($y div 100) + floor($y div 400) - 
        32045"/>

  </xsl:template>

<xsl:template name="date:date-difference">
    <xsl:param name="from-year"/>
    <xsl:param name="from-month"/>
    <xsl:param name="from-day"/>
    <xsl:param name="to-year"/>
    <xsl:param name="to-month"/>
    <xsl:param name="to-day"/>

    <xsl:variable name="jd1">
      <xsl:call-template name="date:calculate-julian-day">
        <xsl:with-param name="year" select="$from-year"/>
          <xsl:with-param name="month" select="$from-month"/>
          <xsl:with-param name="day" select="$from-day"/>
      </xsl:call-template>
    </xsl:variable>

    <xsl:variable name="jd2">
      <xsl:call-template name="date:calculate-julian-day">
        <xsl:with-param name="year" select="$to-year"/>
          <xsl:with-param name="month" select="$to-month"/>
          <xsl:with-param name="day" select="$to-day"/>
      </xsl:call-template>
    </xsl:variable>

    <xsl:value-of select="$jd1 - $jd2"/>
</xsl:template>

<xsl:template name="date:julian-day-to-gregorian-date">
    <xsl:param name="j-day"/>

    <xsl:variable name="a" select="$j-day + 32044"/>
    <xsl:variable name="b" select="floor((4 * $a + 3) div 146097)"/>
    <xsl:variable name="c" select="$a - 146097 * floor($b div 4)"/>

    <xsl:call-template name="date:julian-or-gregorian-date-elem">
        <xsl:with-param name="b" select="$b"/>
        <xsl:with-param name="c" select="$c"/>
    </xsl:call-template>

</xsl:template>

</xsl:stylesheet>
