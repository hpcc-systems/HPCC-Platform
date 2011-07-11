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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:template match="/" mode="htmlbody">
    <table width="100%">
      <tbody>
        <tr>
          <td></td>
        </tr>
      </tbody>
    </table>
    <br />
    
    <table width="100%">
      <tr>
        <td width="80%" valign="top">
          <object id="pluginLHS" type="application/x-hpccsystemsgraphviewcontrol" standby="Loading HPCC Systems GraphView Control..."
              width="100%">
          </object>
                <div id="zoom-slider-bg" class="yui-h-slider" tabindex="-1" title="Zoom Graph" style="text-align:left;">
                  <div id="zoom-slider-thumb" class="yui-slider-thumb">
                    <img src="/esp/files/yui/build/slider/assets/thumb-fader.gif" />
                  </div>
                </div>
          <table width="50%">
            <tr>
              <td>
                <div style="width:24px;height:24px;">
                  <span name="loadingMsg" id="loadingMsg" style="display:none; visibility:hidden;float:left;">
                    <img src="/esp/files/yui/build/assets/skins/sam/wait.gif" />
                    <xsl:text>&#160;</xsl:text>
                  </span>
                </div>
              </td>
              <td>
                <div name="autoSpan" id="autoSpan" style="display:none; visibility:hidden;float:left;">
                  <input value="1" id="auto" type="checkbox" onclick="pause_resume()">
                    <xsl:text>Auto Refresh</xsl:text>
                  </input>
                </div>
              </td>
            </tr>
            <tr>
              <td>
                <div name="Timings" id="Timings" style="display:none; visibility:hidden;">
                  <xsl:text>[</xsl:text>
                  <a name="TimingsLink" id="TimingsLink" style="cursor:hand;" href="javascript:void(0)"
                     onclick="showTimings();">Timings</a>
                  <xsl:text>]</xsl:text>
                </div>
              </td>
              <td>
                <div name="Stats" id="Stats" style="display:none; visibility:hidden">
                  <xsl:text>[</xsl:text>
                  <a name="StatsLink" id="StatsLink" href="javascript:void(0)"
                     onclick="showGraphStats(); return false;">Stats...</a>
                  <xsl:text>]</xsl:text>
                </div>
              </td>
            </tr>
          </table>
          <table>
            <tr>
              <td>
                <div id="install_div" style="text-align:right; border-color: grey; display:none; visibility:hidden">
                  <div id="no_control_msg" style="display:none; visibility:hidden">Graph Control needs to be installed to view activity graphs.</div>
                  Your Version:<span id="current_version">Not installed.</span><br />
                  Server Version:<span id="server_version">20110523</span><br />
                  <a href="/WsRoxieQuery/BrowseResources">HPCC Tools Download Page</a>
                </div>
              </td>
            </tr>
          </table>
          
        </td>
        <td valign="top">
          <object id="pluginRHS" type="application/x-hpccsystemsgraphviewcontrol" standby="Loading HPCC Systems GraphView Control..." style="width:100%; height:500px;">
          </object>
          <div id="zoom2-slider-bg" class="yui-h-slider" tabindex="-1" title="Zoom Mini Graph" style="text-align:left;">
            <div id="zoom2-slider-thumb" class="yui-slider-thumb">
              <img src="/esp/files/yui/build/slider/assets/thumb-fader.gif" />
            </div>
          </div>
          <p id="SelectVertex" style="text-align:right; display:none; visibility:hidden;">
            <span>
              <a name="CurrentNode" id="CurrentNode" href="javascript:void(0)" onclick="selectGraphSubGraph(currentgraph.substring(5), currentgraphnode); return false;">...</a>
            </span>
          </p>
          <span name="findNodeBlock" id="findNodeBlock" style="display:none; visibility:hidden">
            <xsl:text>Find:&#160;</xsl:text>
            <input type="text" name="findgvcId" id="findgvcId" class="input" onkeypress="return checkFindEnter('findgvcId')"></input>
            <xsl:text>&#160;</xsl:text>
            <input type="button" class="button" name="findBtn" id="findBtn"
            onclick="findGraphVertex(document.getElementById('findgvcId').value)" value="Find"></input>
            <xsl:text>&#160;</xsl:text>
            <input type="button" class="button" name="findIdBtn" id="findIdBtn"
            onclick="selectVertex(document.getElementById('findgvcId').value)" value="Find Id"></input>
            <xsl:text>&#160;</xsl:text>
            <input type="button" name="resetFindBtn" id="resetFindBtn" title="Clear Find Results" onclick="resetFind()" value="Clear"></input>
          </span>

          <p id="props">&#160;</p>
        </td>
      </tr>
    </table>

    <script type="text/javascript"> 
var slider;
var slider2;
(function() {
    var Event = YAHOO.util.Event,
        Dom   = YAHOO.util.Dom,
        lang  = YAHOO.lang,
        bg="zoom-slider-bg", thumb="zoom-slider-thumb", bg2="zoom2-slider-bg", thumb2="zoom2-slider-thumb";        
 
    // The slider can move 0 pixels up
    var topConstraint = 0;
 
    // The slider can move 200 pixels down
    var bottomConstraint = 200;
 
    // Custom scale factor for converting the pixel offset into a real value
    var scaleFactor = 1;
 
    // The amount the slider moves when the value is changed with the arrow
    // keys
    var keyIncrement = 20;
 
    Event.onDOMReady(function() {
 
        slider = YAHOO.widget.Slider.getHorizSlider(bg, 
                         thumb, topConstraint, bottomConstraint);
 
        slider.getRealValue = function() {
            return Math.round(this.getValue() * scaleFactor);
        }
 
        slider.subscribe("change", function(offsetFromStart) {
            setScale(slider.getRealValue());
 
        });
 
        slider.subscribe("slideStart", function() {
            });
 
        slider.subscribe("slideEnd", function() {
            });
 
        // set an initial value
        slider.setValue(100);
 
        // Use setValue to reset the value to white:
        Event.on("putval", "click", function(e) {
            slider.setValue(100, false); //false here means to animate if possible
        });

        slider2 = YAHOO.widget.Slider.getHorizSlider(bg2, 
                         thumb2, topConstraint, bottomConstraint);
 
        slider2.getRealValue = function() {
            return Math.round(this.getValue() * scaleFactor);
        }
 
        slider2.subscribe("change", function(offsetFromStart) {
            setScaleRHS(slider2.getRealValue());
 
        });
 
        slider2.subscribe("slideStart", function() {
            });
 
        slider2.subscribe("slideEnd", function() {
            });
 
        // set an initial value
        slider2.setValue(100);
 
        // Use setValue to reset the value to white:
        Event.on("putval", "click", function(e) {
            slider2.setValue(100, false); //false here means to animate if possible
        });

    });
})();
</script>
                
    
    <span id="GVCLink"/>

    <table>
      <tr>
        <td colspan="2">
          <iframe id="xgmmlFrame" frameborder="0" scrolling="no" style="display:none; visibility:hidden">
          </iframe>
        </td>
      </tr>
    </table>

  </xsl:template>
</xsl:stylesheet>
