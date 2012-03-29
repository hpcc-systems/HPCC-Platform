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
    <xsl:output method="html"/>
    <xsl:variable name="chaturl0" select="ActivityResponse/ChatURL"/>
    <xsl:variable name="sortby" select="ActivityResponse/SortBy"/>
    <xsl:variable name="descending" select="ActivityResponse/Descending"/>
    <xsl:template match="ActivityResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <meta http-equiv="Refresh" content="100"/>
                <link rel="stylesheet" type="text/css" href="/esp/files/default.css"/>
                <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/fonts/fonts-min.css" />
                <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/menu/assets/skins/sam/menu.css" />
                <link rel="stylesheet" type="text/css" href="/esp/files/yui/build/button/assets/skins/sam/button.css" />
                <link rel="stylesheet" type="text/css" href="/esp/files/css/espdefault.css" />
                <link rel="stylesheet" type="text/css" href="/esp/files/css/eclwatch.css" />
                <xsl:text disable-output-escaping="yes"><![CDATA[
                    <script type="text/javascript" src="/esp/files/scripts/espdefault.js"></script>
                    <script type="text/javascript" src="/esp/files/yui/build/yahoo-dom-event/yahoo-dom-event.js"></script>
                    <script type="text/javascript" src="/esp/files/yui/build/container/container_core-min.js"></script>
                    <script type="text/javascript" src="/esp/files/yui/build/menu/menu-min.js"></script>
                ]]></xsl:text>
                <title>EclWatch</title>
                <script type="text/javascript">
                    var chatUrl='<xsl:value-of select="$chaturl0"/>';
                    var sortBy='<xsl:value-of select="$sortby"/>';
                    var descending='<xsl:value-of select="$descending"/>';
                </script>
                <script language="JavaScript1.2" id="menuhandlers">
                    <xsl:text disable-output-escaping="yes"><![CDATA[
                        function onLoad()
                        {
                            var selection = document.getElementById("sortClusters");
                            if (selection != NaN)
                            {
                                if (sortBy == 'Name' && descending == 1)
                                    selection.options[1].selected="selected";
                                else if (sortBy == 'Size' && descending == 0)
                                    selection.options[2].selected="selected";
                                else if (sortBy == 'Size' && descending == 1)
                                    selection.options[3].selected="selected";
                                else
                                    selection.options[0].selected="selected";
                            }
                        }

                        function sortClustersChanged(sortClusterBy)
                        {
                            if (sortClusterBy == 2)
                                document.location.href = "/WsSmc/Activity?SortBy=Name&Descending=1";
                            else if (sortClusterBy == 3)
                                document.location.href = "/WsSmc/Activity?SortBy=Size";
                            else if (sortClusterBy == 4)
                                document.location.href = "/WsSmc/Activity?SortBy=Size&Descending=1";
                            else
                                document.location.href = "/WsSmc/Activity?SortBy=Name";
                        }

                        function commandQueue(action,isThor,cluster,queue,wuid)
                        {
                            document.getElementById("ClusterType").value=isThor;
                            document.getElementById("Cluster").value=cluster;
                            document.getElementById("QueueName").value=queue;
                            document.getElementById("Wuid").value=wuid || '';
                            document.forms["queue"].action='/WsSMC/'+action;
                            document.forms["queue"].submit();
                        }

                        var oMenu;

                        function queuePopup(isRoxie,cluster,queue,paused,stopped, PosId)
                        {
                            isThor = 0;
                            if (isRoxie < 1)
                                isThor = 1;
                            function clearQueue()
                            {
                                if(confirm('Clear the queue for cluster: '+cluster))
                                    commandQueue("ClearQueue",isThor,cluster,queue);
                            }
                            function stopQueue()
                            {
                                if(confirm('Stop the queue for cluster '+cluster))
                                    commandQueue("StopQueue",isThor,cluster,queue);
                            }
                            function pauseQueue()
                            {
                                commandQueue("PauseQueue",isThor,cluster,queue);
                            }
                            function resumeQueue()
                            {
                                commandQueue("ResumeQueue",isThor,cluster,queue);
                            }
                            function showUsage()
                            {
                                document.location.href='/WsWorkunits/WUJobList?form_&Cluster='+cluster+'&Range=30';
                            }
                            var xypos = YAHOO.util.Dom.getXY('mn' + PosId);
                            if (oMenu) {
                                oMenu.destroy();
                            }
                            oMenu = new YAHOO.widget.Menu("logicalfilecontextmenu", {position: "dynamic", xy: xypos} );
                            oMenu.clearContent();

                            oMenu.addItems([
                                { text: "Pause", onclick: { fn: pauseQueue }, disabled: !paused && !stopped ? false : true },
                                { text: "Resume", onclick: { fn: resumeQueue }, disabled: paused && !stopped ? false : true },
                                //{ text: "Stop", onclick: { fn: stopQueue }, disabled: paused && stopped ? false : true },
                                { text: "Clear", onclick: { fn: clearQueue } },
                                ///{ text: "Usage", onclick: { fn: showUsage }, disabled: isFF ? true : false }
                                { text: "Usage", onclick: { fn: showUsage } }
                            ]);

                            oMenu.render("dfulogicalfilemenu");
                            oMenu.show();

                            return false;
                        }

                        function activePopup(type, isRoxie, cluster,queue,wuid,highpriority, PosId)
                        {
                            isThor = 0;
                            if (isRoxie < 1)
                                isThor = 1;
                            function abortWuid()
                            {
                                if(confirm('Abort '+wuid+'?'))
                                {
                                    document.location="/WsWorkunits/WUAction?ActionType=Abort&Wuids_i1="+wuid;
                                }
                            }

                            function pauseWuid()
                            {
                                if(confirm('Pause '+wuid+'?'))
                                {
                                    document.location="/WsWorkunits/WUAction?ActionType=Pause&Wuids_i1="+wuid;
                                }
                            }

                            function pauseWuidNow()
                            {
                                if(confirm('Pause '+wuid+' now?'))
                                {
                                    document.location="/WsWorkunits/WUAction?ActionType=PauseNow&Wuids_i1="+wuid;
                                }
                            }

                            function resumeWuid()
                            {
                                if(confirm('Resume '+wuid+'?'))
                                {
                                    document.location="/WsWorkunits/WUAction?ActionType=Resume&Wuids_i1="+wuid;
                                }
                            }

                            function setHighPriority()
                            {
                                commandQueue("SetJobPriority?Priority=High",isThor, cluster,queue,wuid);
                            }

                            function setNormalPriority()
                            {
                                commandQueue("SetJobPriority?Priority=Normal",isThor, cluster,queue,wuid);
                            }

                            var xypos = YAHOO.util.Dom.getXY('amn' + PosId);
                            if (oMenu) {
                                oMenu.destroy();
                            }
                            oMenu = new YAHOO.widget.Menu("logicalfilecontextmenu", {position: "dynamic", xy: xypos} );
                            oMenu.clearContent();

                            if (type < 1)
                            {
                                oMenu.addItems([
                                    { text: "Abort", onclick: { fn: abortWuid } },
                                    { text: "High Priority", onclick: { fn: highpriority ? setNormalPriority : setHighPriority }, checked: highpriority ? true : false }
                                ]);
                            }
                            else if (type < 2)
                            {
                                oMenu.addItems([
                                    { text: "Pause", onclick: { fn: pauseWuid } },
                                    { text: "PauseNow", onclick: { fn: pauseWuidNow } },
                                    { text: "Abort", onclick: { fn: abortWuid } },
                                    { text: "High Priority", onclick: { fn: highpriority ? setNormalPriority : setHighPriority }, checked: highpriority ? true : false }
                                ]);
                            }
                            else
                            {
                                oMenu.addItems([
                                    { text: "Resume", onclick: { fn: resumeWuid } },
                                    { text: "Abort", onclick: { fn: abortWuid } },
                                    { text: "High Priority", onclick: { fn: highpriority ? setNormalPriority : setHighPriority }, checked: highpriority ? true : false }
                                ]);
                            }

                            oMenu.render("dfulogicalfilemenu");
                            oMenu.show();
                            return false;
                        }

                        function wuidPopup(isRoxie, cluster,queue,wuid,prev,next,highpriority, PosId)
                        {
                            isThor = 0;
                            if (isRoxie < 1)
                                isThor = 1;
                            function moveupWuid()
                            {
                                commandQueue("MoveJobUp",isThor, cluster,queue,wuid);
                            }
                            function movedownWuid()
                            {
                                commandQueue("MoveJobDown",isThor, cluster,queue,wuid);
                            }
                            function movefrontWuid()
                            {
                                commandQueue("MoveJobFront",isThor, cluster,queue,wuid);
                            }
                            function movebackWuid()
                            {
                                commandQueue("MoveJobBack",isThor, cluster,queue,wuid);
                            }
                            function removeWuid()
                            {
                                if(confirm('Remove '+wuid))
                                    commandQueue("RemoveJob",isThor, cluster,queue,wuid);
                            }
                            function setHighPriority()
                            {
                                commandQueue("SetJobPriority?Priority=High",isThor, cluster,queue,wuid);
                            }

                            function setNormalPriority()
                            {
                                commandQueue("SetJobPriority?Priority=Normal",isThor, cluster,queue,wuid);
                            }

                            var xypos = YAHOO.util.Dom.getXY('wmn' + PosId);
                            if (oMenu) {
                                oMenu.destroy();
                            }
                            oMenu = new YAHOO.widget.Menu("logicalfilecontextmenu", {position: "dynamic", xy: xypos} );
                            oMenu.clearContent();

                            oMenu.addItems([
                                { text: "Move Up", onclick: { fn: moveupWuid }, disabled: prev ? false : true },
                                { text: "Move Down", onclick: { fn: movedownWuid }, disabled: next ? false : true },
                                { text: "Move Top", onclick: { fn: movefrontWuid }, disabled: prev ? false : true },
                                { text: "Move Bottom", onclick: { fn: movebackWuid }, disabled: next ? false : true },
                                { text: "Remove", onclick: { fn: removeWuid } },
                                { text: "High Priority", onclick: { fn: highpriority ? setNormalPriority : setHighPriority }, checked: highpriority ? true : false }
                            ]);

                            oMenu.render("dfulogicalfilemenu");
                            oMenu.show();
                            return false;
                        }

                        function popup0()
                        {
                            mywindow = window.open (chatUrl, "mywindow", "location=0,status=1,scrollbars=1,resizable=1,width=400,height=200");
                            if (mywindow.opener == null)
                                mywindow.opener = window;
                            mywindow.focus();
                            return false;
                        }

                        var showSetBanner = 0;
                        function SetBanner()
                        {
                            obj = document.getElementById('SetBannerFrame');
                            if (obj)
                            {
                                showSetBanner = (showSetBanner > 0) ? 0: 1;
                                if (showSetBanner > 0)
                                {
                                    obj.style.display = 'inline';
                                    obj.style.visibility = 'visible';
                                }
                                else
                                {
                                    obj.style.display = 'none';
                                    obj.style.visibility = 'hidden';
                                }
                            }
                        }

                        function handleSetBannerForm(item, value)
                        {
                            if (item == 1)
                            {
                                obj = document.getElementById('ChatURL');
                                if (obj)
                                {
                                    if (value)
                                        obj.disabled = false;
                                    else
                                        obj.disabled = true;
                                }
                            }
                            else
                            {
                                obj = document.getElementById('BannerContent');
                                if (obj)
                                {
                                    if (value)
                                        obj.disabled = false;
                                    else
                                        obj.disabled = true;
                                }
                                obj = document.getElementById('BannerColor');
                                if (obj)
                                {
                                    if (value)
                                        obj.disabled = false;
                                    else
                                        obj.disabled = true;
                                }
                                obj = document.getElementById('BannerSize');
                                if (obj)
                                {
                                    if (value)
                                        obj.disabled = false;
                                    else
                                        obj.disabled = true;
                                }
                                obj = document.getElementById('BannerScroll');
                                if (obj)
                                {
                                    if (value)
                                        obj.disabled = false;
                                    else
                                        obj.disabled = true;
                                }
                            }
                        }

                        function checkSize(strString)
                        {
                            var strChar;
                            var strValidChars = "0123456789";
                            var bOK = true;

                            if (strString.length == 0) return false;

                            for (i = 0; i < strString.length && bOK == true; i++)
                            {
                                strChar = strString.charAt(i);
                                if (strValidChars.indexOf(strChar) == -1)
                                {
                                    bOK = false;
                                }
                                else if (i == 0 && strChar == '0')
                                {
                                    bOK = false;
                                }
                            }

                            return bOK;
                        }

                        function trimAll(sString)
                        {
                            while (sString.substring(0,1) == ' ')
                            {
                                sString = sString.substring(1, sString.length);
                            }
                            while (sString.substring(sString.length-1, sString.length) == ' ')
                            {
                                sString = sString.substring(0,sString.length-1);
                            }
                            return sString;
                        }

                        function handleSubmitBtn()
                        {
                            showBanner = 0;
                            banner = "";
                            bannerScroll = "2";
                            bannerSize = "12";
                            bannerColor = "red";
                            showChatLink = 0;
                            chatURL = "";

                            obj = document.getElementById("CB_BannerContent");
                            obj1 = document.getElementById("BannerContent");
                            obj2 = document.getElementById("BannerColor");
                            obj3 = document.getElementById("BannerSize");
                            obj4 = document.getElementById("BannerScroll");
                            if (obj)
                            {
                                if (obj.checked)
                                {
                                    if (obj1 && (obj1.value != ''))
                                    {
                                        showBanner = 1;
                                    }
                                    else
                                    {
                                        alert("Banner content should not be empty.");
                                        return false;
                                    }
                                }
                                if (obj1 && (obj1.value != ''))
                                {
                                    banner = trimAll(obj1.value);
                                }
                                if (obj2 && (obj2.value != ''))
                                {
                                    bannerColor = obj2.value;
                                }
                                if (obj3 && (obj3.value != ''))
                                {
                                    if (checkSize(obj3.value))
                                        bannerSize = obj3.value;
                                    else
                                    {
                                        alert("Incorrect size input!");
                                        return false;
                                    }
                                }
                                if (obj4 && (obj4.value != ''))
                                {
                                    if (checkSize(obj4.value))
                                        bannerScroll = obj4.value;
                                    else
                                    {
                                        alert("Incorrect scroll amount input!");
                                        return false;
                                    }
                                }
                            }

                            obj2 = document.getElementById("CB_ChatURL");
                            obj3 = document.getElementById("ChatURL");
                            if (obj2)
                            {
                                if (obj2.checked)
                                {
                                    chatURL0 = '';
                                    if (obj3 && (obj3.value != ''))
                                        chatURL0 = trimAll(obj3.value);

                                    if (chatURL0 != '')
                                    {
                                        showChatLink = 1;
                                        if (chatURL0.indexOf('http') == 0)
                                            chatURL = chatURL0;
                                        else
                                            chatURL = 'http://'+ chatURL0;
                                    }
                                    else
                                    {
                                        alert("Chat URL should not be empty.");
                                        return;
                                    }
                                }
                                else if (obj3 && (obj3.value != ''))
                                {
                                    chatURL = trimAll(obj3.value);
                                }
                            }
                            var href='/WsSMC/Activity?FromSubmitBtn=true&BannerAction='+showBanner + '&EnableChatURL='+showChatLink;
                            href += ('&ChatURL=' + chatURL);
                            href += ('&BannerContent=' + banner);
                            href += ('&BannerColor=' + bannerColor);
                            href += ('&BannerSize=' + bannerSize);
                            href += ('&BannerScroll=' + bannerScroll);
                            document.location.href=href;
                        }
                    ]]></xsl:text>
                </script>
            </head>
            <body class="yui-skin-sam" onload="nof5();onLoad()">
                <form>
                    <table>
                        <xsl:if test="ShowChatURL = 1">
                            <tr>
                                <td>
                                    <a style="padding-right:2" href="">
                                        <xsl:attribute name="onclick">
                                            <xsl:text>return popup0();</xsl:text>
                                        </xsl:attribute>
                                        Launch JWChat
                                    </a>
                                    <!--center>
                                <iframe src="http://jwchat.org" 
                                    width="472" height="320" scrolling="no" frameborder="2"/>
                                </center-->
                                </td>
                            </tr>
                        </xsl:if>
                        <xsl:if test="ShowBanner = 1">
                            <tr>
                                <td>
                                    <!--marquee width="800" height="20" direction="left" scrollamount="1" scrolldelay="20"-->
                                    <marquee width="800" direction="left" scrollamount="{BannerScroll}" >
                                        <font color="{BannerColor}" family="Verdana" size="{BannerSize}">
                                            <xsl:value-of select="BannerContent"/>
                                        </font>
                                    </marquee>
                                </td>
                            </tr>
                        </xsl:if>
                    </table>
                </form>
                <table width="100%">
                    <tbody>
                        <tr>
                            <td align="left">
                                <xsl:choose>
                                    <xsl:when test="UserPermission = 0">
                                        <A href="javascript:void(0)" onclick="SetBanner();">
                                            <h3>Existing Activity on Servers:</h3>
                                        </A>
                                    </xsl:when>
                                    <xsl:otherwise>
                                        <h3>
                                            Existing Activity on Servers:
                                        </h3>
                                    </xsl:otherwise>
                                </xsl:choose>
                            </td>
                            <td align="right">
                                <select id="sortClusters" name="sortClusters" onchange="sortClustersChanged(options[selectedIndex].value);">
                                    <option value="1">Sort clusters by name ascending</option>
                                    <option value="2">Sort clusters by name descending</option>
                                    <option value="3">Sort clusters by size ascending</option>
                                    <option value="4">Sort clusters by size descending</option>
                                </select>
                            </td>
                        </tr>
                    </tbody>
                </table>
                <form id="queue" action="/WsSMC" method="post">
                    <input type="hidden" name="ClusterType" id="ClusterType" value=""/>
                    <input type="hidden" name="Cluster" id="Cluster" value=""/>
                    <input type="hidden" name="QueueName" id="QueueName" value=""/>
                    <input type="hidden" name="Wuid" id="Wuid" value=""/>
                    <!--table class="clusters" border="1" frame="box" rules="groups">
                        <tr>
                            <td style="background-color:#AAAAAA">
                                <table class="clusters" border="1" frame="box" rules="groups">
                                    <colgroup>
                                        <col width="250" class="cluster"/>
                                    </colgroup>
                                    <colgroup>
                                        <col width="200" class="cluster"/>
                                    </colgroup>
                                    <colgroup>
                                        <col width="150" class="cluster"/>
                                    </colgroup>
                                    <colgroup>
                                        <col width="500" class="cluster"/>
                                    </colgroup>
                                    <tr>
                                        <th>Active workunit</th>
                                        <th>State</th>
                                        <th>Owner</th>
                                        <th>Job name</th>  
                                    </tr>
                                </table>
                            </td>
                        </tr>
                    </table-->
                    <xsl:for-each select="ThorClusters/ThorCluster">
                        <xsl:call-template name="show-queue">
                            <xsl:with-param name="workunits" select="//Running/ActiveWorkunit[Server='ThorMaster' and QueueName=current()/QueueName]"/>
                            <xsl:with-param name="cluster" select="ClusterName"/>
                            <xsl:with-param name="queue" select="QueueName"/>
                            <xsl:with-param name="status" select="QueueStatus"/>
                            <xsl:with-param name="status2" select="QueueStatus2"/>
                            <xsl:with-param name="command" select="DoCommand"/>
                            <xsl:with-param name="thor" select="ThorLCR"/>
                        </xsl:call-template>
                    </xsl:for-each>

                    <xsl:choose>
                        <xsl:when test="count(//ThorClusters/ThorCluster) &gt; 0">
                            <xsl:for-each select="RoxieClusters/RoxieCluster">
                                <xsl:call-template name="show-queue">
                                    <xsl:with-param name="workunits" select="//Running/ActiveWorkunit[Server='RoxieServer' and QueueName=current()/QueueName]"/>
                                    <xsl:with-param name="cluster" select="ClusterName"/>
                                    <xsl:with-param name="queue" select="QueueName"/>
                                    <xsl:with-param name="status" select="QueueStatus"/>
                                    <xsl:with-param name="status2" select="QueueStatus2"/>
                                    <xsl:with-param name="command" select="DoCommand"/>
                                    <xsl:with-param name="roxie" select="'1'"/>
                                    <xsl:with-param name="showtitle" select="'0'"/>
                                </xsl:call-template>
                            </xsl:for-each>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:for-each select="RoxieClusters/RoxieCluster">
                                <xsl:call-template name="show-queue">
                                    <xsl:with-param name="workunits" select="//Running/ActiveWorkunit[Server='RoxieServer' and QueueName=current()/QueueName]"/>
                                    <xsl:with-param name="cluster" select="ClusterName"/>
                                    <xsl:with-param name="queue" select="QueueName"/>
                                    <xsl:with-param name="status" select="QueueStatus"/>
                                    <xsl:with-param name="status2" select="QueueStatus2"/>
                                    <xsl:with-param name="command" select="DoCommand"/>
                                    <xsl:with-param name="roxie" select="'1'"/>
                                    <xsl:with-param name="showtitle" select="'1'"/>
                                </xsl:call-template>
                            </xsl:for-each>
                        </xsl:otherwise>
                    </xsl:choose>

                    <xsl:call-template name="show-server">
                        <xsl:with-param name="workunits" select="//Running/ActiveWorkunit[Server='ECLagent' and not(Wuid=//Running/ActiveWorkunit[Server='ThorMaster']/Wuid)]"/>
                    </xsl:call-template>

                    <xsl:call-template name="show-server">
                        <xsl:with-param name="workunits" select="//Running/ActiveWorkunit[Server='ECLCCserver']"/>
                    </xsl:call-template>

                    <xsl:call-template name="show-server">
                        <xsl:with-param name="workunits" select="//Running/ActiveWorkunit[Server='DFUserver']"/>
                    </xsl:call-template>
                    <xsl:apply-templates select="HoleClusters"/>
                    <xsl:apply-templates select="DFUJobs"/>
                </form>
                <xsl:if test="UserPermission = 0">
                    <span id="SetBannerFrame"   style="display:none; visibility:hidden">
                        <form id="SetBannerForm">
                            <table>
                                <tr>
                                    <xsl:choose>
                                        <xsl:when test="ShowBanner = 0">
                                            <td valign="top">
                                                <input type="checkbox" id="CB_BannerContent" title="Display Banner:" onclick="handleSetBannerForm(0, this.checked)"/>
                                            </td>
                                            <td valign="top">Banner:</td>
                                            <td>
                                                <textarea rows="4" cols="20" style="width:520px" name="BannerContent" id="BannerContent" disabled="true">
                                                    <xsl:value-of select="BannerContent"/>&#160;
                                                </textarea>
                                            </td>
                                            <td valign="top">Color:</td>
                                            <td valign="top">
                                                <input type="text" name="BannerColor" id="BannerColor" value="{BannerColor}" size="10" disabled="true"/>
                                            </td>
                                            <td valign="top">Font Size:</td>
                                            <td valign="top">
                                                <input type="text" name="BannerSize" id="BannerSize" value="{BannerSize}" size="10" disabled="true"/>
                                            </td>
                                            <td valign="top">Scroll Amount:</td>
                                            <td valign="top">
                                                <input type="text" name="BannerScroll" id="BannerScroll" value="{BannerScroll}" size="10" disabled="true"/>
                                            </td>
                                        </xsl:when>
                                        <xsl:otherwise>
                                            <td valign="top">
                                                <input type="checkbox" id="CB_BannerContent" title="Display Banner:" checked="{ShowBanner}" onclick="handleSetBannerForm(0, this.checked)"/>
                                            </td>
                                            <td valign="top">Banner:</td>
                                            <td>
                                                <textarea rows="4" STYLE="width:520" name="BannerContent" id="BannerContent">
                                                    <xsl:value-of select="BannerContent"/>&#160;
                                                </textarea>
                                            </td>
                                            <td valign="top">Color:</td>
                                            <td valign="top">
                                                <input type="text" name="BannerColor" id="BannerColor" value="{BannerColor}" size="10"/>
                                            </td>
                                            <td valign="top">Font Size:</td>
                                            <td valign="top">
                                                <input type="text" name="BannerSize" id="BannerSize" value="{BannerSize}" size="10"/>
                                            </td>
                                            <td valign="top">Scroll Amount:</td>
                                            <td valign="top">
                                                <input type="text" name="BannerScroll" id="BannerScroll" value="{BannerScroll}" size="10"/>
                                            </td>
                                        </xsl:otherwise>
                                    </xsl:choose>
                                </tr>
                                <tr>
                                    <xsl:choose>
                                        <xsl:when test="ShowChatURL = 0">
                                            <td>
                                                <input type="checkbox" id="CB_ChatURL" title="Display Chat Link:" onclick="handleSetBannerForm(1, this.checked)"/>
                                            </td>
                                            <td valign="top">Chat URL:</td>
                                            <td>
                                                <input type="text" name="ChatURL" id="ChatURL" value="{ChatURL}" size="80" disabled="true"/>
                                            </td>
                                        </xsl:when>
                                        <xsl:otherwise>
                                            <td>
                                                <input type="checkbox" id="CB_ChatURL" title="Display Chat Link:" checked="{ShowChatURL}" onclick="handleSetBannerForm(1, this.checked)"/>
                                            </td>
                                            <td valign="top">Chat URL:</td>
                                            <td>
                                                <input type="text" name="ChatURL" id="ChatURL" value="{ChatURL}" size="80"/>
                                            </td>
                                        </xsl:otherwise>
                                    </xsl:choose>
                                </tr>
                                <tr>
                                    <td/>
                                    <td>
                                        <input type="button" class="sbutton" id="submitBtn" name="submitBtn" value="Submit" onclick="return handleSubmitBtn()"/>
                                    </td>
                                </tr>
                            </table>
                        </form>
                    </span>
                </xsl:if>
                <div id="menu" style="position:absolute;visibility:hidden;top:0;left:0"></div>
                <div id="dfulogicalfilemenu" />
            </body>
        </html>
    </xsl:template>
    <xsl:template match="Build">
    </xsl:template>

    <xsl:template name="show-server">
        <xsl:param name="workunits"/>
        <xsl:if test="count($workunits)>0">
            <xsl:variable name="server" select="$workunits[1]/Server"/>
            <xsl:variable name="cluster" select="$workunits[1]/Instance"/>
            <xsl:variable name="queue" select="$workunits[1]/QueueName"/>
            <xsl:choose>
                <xsl:when test="count(//ThorClusters/ThorCluster) &gt; 0">
                    <xsl:call-template name="show-queue">
                        <xsl:with-param name="workunits" select="$workunits[Instance=$cluster]"/>
                        <xsl:with-param name="cluster" select="$cluster"/>
                        <xsl:with-param name="server" select="$server"/>
                        <xsl:with-param name="queue" select="$queue"/>
                        <xsl:with-param name="showtitle" select="'0'"/>
                    </xsl:call-template>
                </xsl:when>
                <xsl:when test="count(//RoxieClusters/RoxieCluster) &gt; 0">
                    <xsl:call-template name="show-queue">
                        <xsl:with-param name="workunits" select="$workunits[Instance=$cluster]"/>
                        <xsl:with-param name="cluster" select="$cluster"/>
                        <xsl:with-param name="server" select="$server"/>
                        <xsl:with-param name="queue" select="$queue"/>
                        <xsl:with-param name="showtitle" select="'0'"/>
                    </xsl:call-template>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:call-template name="show-queue">
                        <xsl:with-param name="workunits" select="$workunits[Instance=$cluster]"/>
                        <xsl:with-param name="cluster" select="$cluster"/>
                        <xsl:with-param name="server" select="$server"/>
                        <xsl:with-param name="queue" select="$queue"/>
                    </xsl:call-template>
                </xsl:otherwise>
            </xsl:choose>
            <xsl:if test="count($workunits[Instance!=$cluster])">
                <xsl:call-template name="show-server">
                    <xsl:with-param name="workunits" select="$workunits[Instance!=$cluster]"/>
                </xsl:call-template>
            </xsl:if>
        </xsl:if>
    </xsl:template>

    <xsl:template name="show-queue">
        <xsl:param name="workunits"/>
        <xsl:param name="cluster"/>
        <xsl:param name="queue"/>
        <xsl:param name="server" select="''"/>
        <xsl:param name="status" select="''"/>
        <xsl:param name="status2" select="6"/>
        <xsl:param name="command" select="0"/>
        <xsl:param name="thor" select="'0'"/>
        <xsl:param name="roxie" select="'0'"/>
        <xsl:param name="showtitle" select="'1'"/>
        <table class="clusters" border="2" frame="box" rules="groups" style="margin-bottom:5px">
            <tr>
                <xsl:choose>
                    <xsl:when test="$status='paused' or $status='stopped'">
                        <td valign="top">
                            <xsl:if test="number($command)">
                                <xsl:variable name="popup">
                                    return queuePopup('<xsl:value-of select="$roxie"/>','<xsl:value-of select="$cluster"/>','<xsl:value-of select="$queue"/>',<xsl:value-of select="$status='paused'"/>, <xsl:value-of select="$status='stopped'"/>, <xsl:value-of select="position()"/>)
                                </xsl:variable>
                                <xsl:attribute name="oncontextmenu">
                                    <xsl:value-of select="$popup"/>
                                </xsl:attribute>
                                <a id="mn{position()}" class="configurecontextmenu" title="Configure" onclick="{$popup}">&#160;</a>
                            </xsl:if>
                            <xsl:choose>
                                <xsl:when test="$roxie='0'">
                                    <a>
                                        <xsl:choose>
                                            <xsl:when test="$status2='1'">
                                                <xsl:attribute name="class">thorrunningpausedqueuejobs</xsl:attribute>
                                                <xsl:attribute name="title">Queue paused - Thor running</xsl:attribute>
                                            </xsl:when>
                                            <xsl:when test="$status2='2'">
                                                <xsl:attribute name="class">thorrunningpausedqueuenojobs</xsl:attribute>
                                                <xsl:attribute name="title">Queue paused - Thor running (all jobs blocked?)</xsl:attribute>
                                            </xsl:when>
                                            <xsl:when test="$status2='3'">
                                                <xsl:attribute name="class">thorstoppedpausedqueuenojobs</xsl:attribute>
                                                <xsl:attribute name="title">Queue paused - Thor stopped (NOC has stopped all ThorMasters on cluster)</xsl:attribute>
                                            </xsl:when>
                                            <xsl:when test="$status2='5'">
                                                <xsl:attribute name="class">thorstoppedrunningqueue</xsl:attribute>
                                                <xsl:attribute name="title">Queue running - Thor Stopped (ThorMasters cannot start?)</xsl:attribute>
                                            </xsl:when>
                                            <xsl:otherwise>
                                                <xsl:attribute name="class">thorrunning</xsl:attribute>
                                                <xsl:attribute name="title">Queue running - Thor running</xsl:attribute>
                                            </xsl:otherwise>
                                        </xsl:choose>
                                        <xsl:attribute name="href">
                                            javascript:go('/WsTopology/TpClusterInfo?Name=<xsl:value-of select="$cluster"/>')
                                        </xsl:attribute>
                                        <xsl:choose>
                                            <xsl:when test="$roxie!='0'">RoxieCluster - </xsl:when>
                                            <xsl:when test="$thor!='0'">ThorCluster - </xsl:when>
                                        </xsl:choose>
                                        <xsl:value-of select="$cluster"/>
                                    </a>
                                </xsl:when>
                                <xsl:otherwise>
                                    <b>
                                        <xsl:choose>
                                            <xsl:when test="$roxie!='0'">RoxieCluster - </xsl:when>
                                            <xsl:when test="$thor!='0'">ThorCluster - </xsl:when>
                                        </xsl:choose>
                                        <xsl:value-of select="$cluster"/>
                                    </b>
                                </xsl:otherwise>
                            </xsl:choose>
                        </td>
                    </xsl:when>
                    <xsl:otherwise>
                        <td valign="top">
                            <xsl:if test="number($command)">
                                <xsl:variable name="popup">
                                    return queuePopup('<xsl:value-of select="$roxie"/>','<xsl:value-of select="$cluster"/>','<xsl:value-of select="$queue"/>',<xsl:value-of select="$status='paused'"/>, <xsl:value-of select="$status='stopped'"/>, <xsl:value-of select="position()"/>)
                                </xsl:variable>
                                <xsl:attribute name="oncontextmenu">
                                    <xsl:value-of select="$popup"/>
                                </xsl:attribute>
                                <a id="mn{position()}" class="configurecontextmenu" title="Configure" onclick="{$popup}">&#160;</a>
                            </xsl:if>
                            <xsl:choose>
                                <xsl:when test="$roxie='0'">
                                    <a>
                                        <xsl:choose>
                                            <xsl:when test="$status2='1'">
                                                <xsl:attribute name="class">thorrunningpausedqueuejobs</xsl:attribute>
                                                <xsl:attribute name="title">Queue paused - Thor running</xsl:attribute>
                                            </xsl:when>
                                            <xsl:when test="$status2='2'">
                                                <xsl:attribute name="class">thorrunningpausedqueuenojobs</xsl:attribute>
                                                <xsl:attribute name="title">Queue paused - Thor running (all jobs blocked?)</xsl:attribute>
                                            </xsl:when>
                                            <xsl:when test="$status2='3'">
                                                <xsl:attribute name="class">thorstoppedpausedqueuenojobs</xsl:attribute>
                                                <xsl:attribute name="title">Queue paused - Thor stopped (NOC has stopped all ThorMasters on cluster)</xsl:attribute>
                                            </xsl:when>
                                            <xsl:when test="$status2='5'">
                                                <xsl:attribute name="class">thorstoppedrunningqueue</xsl:attribute>
                                                <xsl:attribute name="title">Queue running - Thor Stopped (ThorMasters cannot start?)</xsl:attribute>
                                            </xsl:when>
                                            <xsl:otherwise>
                                                <xsl:attribute name="class">thorrunning</xsl:attribute>
                                                <xsl:attribute name="title">Queue running - Thor running</xsl:attribute>
                                            </xsl:otherwise>
                                        </xsl:choose>
                                        <xsl:if test="string-length($status)">
                                            <xsl:attribute name="href">javascript:go('/WsTopology/TpClusterInfo?Name=<xsl:value-of select="$cluster"/>')</xsl:attribute>
                                        </xsl:if>
                                        <xsl:choose>
                                            <xsl:when test="$roxie!='0'">RoxieCluster - </xsl:when>
                                            <xsl:when test="$thor!='0'">ThorCluster - </xsl:when>
                                        </xsl:choose>
                                        <xsl:value-of select="$cluster"/>
                                    </a>
                                </xsl:when>
                                <xsl:otherwise>
                                    <b>
                                        <xsl:choose>
                                            <xsl:when test="$roxie!='0'">RoxieCluster - </xsl:when>
                                            <xsl:when test="$thor!='0'">ThorCluster - </xsl:when>
                                        </xsl:choose>
                                        <xsl:value-of select="$cluster"/>
                                    </b>
                                </xsl:otherwise>
                            </xsl:choose>
                        </td>
                    </xsl:otherwise>
                </xsl:choose>
            </tr>
            <tr>
                <td>
                    <table class="clusters"  border="1" frame="box" rules="groups">
                        <colgroup>
                            <col width="250" class="cluster"/>
                        </colgroup>
                        <colgroup>
                            <col width="200" class="cluster"/>
                        </colgroup>
                        <colgroup>
                            <col width="150" class="cluster"/>
                        </colgroup>
                        <colgroup>
                            <col width="500" class="cluster"/>
                        </colgroup>
                        <xsl:if test="(position()=1 and $showtitle='1')">
                            <tr>
                                <th style="background-color:#DDDDDD">Active workunit</th>
                                <th style="background-color:#DDDDDD">State</th>
                                <th style="background-color:#DDDDDD">Owner</th>
                                <th style="background-color:#DDDDDD">Job name</th>
                            </tr>
                        </xsl:if>
                        <xsl:call-template name="show-queue0">
                            <xsl:with-param name="workunits" select="$workunits"/>
                            <xsl:with-param name="cluster" select="$cluster"/>
                            <xsl:with-param name="server" select="$server"/>
                            <xsl:with-param name="queue" select="$queue"/>
                            <xsl:with-param name="command" select="$command"/>
                            <xsl:with-param name="thor" select="$thor"/>
                            <xsl:with-param name="roxie" select="$roxie"/>
                        </xsl:call-template>
                    </table>
                </td>
            </tr>
        </table>
    </xsl:template>

    <xsl:template name="show-queue0">
        <xsl:param name="workunits"/>
        <xsl:param name="cluster"/>
        <xsl:param name="queue"/>
        <xsl:param name="server" select="''"/>
        <xsl:param name="command" select="0"/>
        <xsl:param name="thor" select="'0'"/>
        <xsl:param name="roxie" select="'0'"/>

        <xsl:variable name="active" select="$workunits[State='running']"/>
        <tbody>
            <xsl:choose>
                <xsl:when test="$workunits[1]">
                    <tr style="border:solid 2 black">
                        <xsl:if test="count($active)">
                            <xsl:for-each select="$active">
                                <xsl:if test="position() > 1">
                                    <xsl:text disable-output-escaping="yes">
                                            <![CDATA[
                                                </tr>
                                                <tr>
                                            ]]>
                                        </xsl:text>
                                </xsl:if>
                                <xsl:apply-templates select=".">
                                    <xsl:with-param name="cluster" select="$cluster"/>
                                    <xsl:with-param name="queue" select="$queue"/>
                                    <xsl:with-param name="command" select="$command"/>
                                    <xsl:with-param name="thor" select="$thor"/>
                                    <xsl:with-param name="roxie" select="$roxie"/>
                                </xsl:apply-templates>
                            </xsl:for-each>
                        </xsl:if>
                    </tr>

                    <xsl:for-each select="$workunits[State!='running']">
                        <tr>
                            <xsl:apply-templates select=".">
                                <xsl:with-param name="server" select="$server"/>
                                <xsl:with-param name="cluster" select="$cluster"/>
                                <xsl:with-param name="queue" select="$queue"/>
                                <xsl:with-param name="command" select="$command"/>
                                <xsl:with-param name="thor" select="$thor"/>
                                <xsl:with-param name="roxie" select="$roxie"/>
                            </xsl:apply-templates>
                        </tr>
                    </xsl:for-each>
                </xsl:when>
                <xsl:otherwise>
                    <td width="1100" colspan='4'>No active workunit</td>
                </xsl:otherwise>
            </xsl:choose>
        </tbody>
    </xsl:template>

    <xsl:template match="ActiveWorkunit">
        <xsl:param name="server" select="''"/>
        <xsl:param name="cluster" select="''"/>
        <xsl:param name="queue" select="''"/>
        <xsl:param name="command" select="0"/>
        <xsl:param name="thor" select="'0'"/>
        <xsl:param name="roxie" select="'0'"/>

        <xsl:variable name="popupid">
            <xsl:choose>
                <xsl:when test="State='running' or State='paused'">amn<xsl:value-of select="Wuid"/>
                </xsl:when>
                <xsl:when test="starts-with(State,'queued')">wmn<xsl:value-of select="Wuid"/>
                </xsl:when>
            </xsl:choose>
        </xsl:variable>
        <xsl:variable name="popup">
            <xsl:choose>
                <xsl:when test="State='running'">
                    <xsl:choose>
                        <xsl:when test="$thor='0' or $thor='noLCR'">
                            return activePopup(0, '<xsl:value-of select="$roxie"/>','<xsl:value-of select="$cluster"/>','<xsl:value-of select="$queue"/>','<xsl:value-of select="Wuid"/>',<xsl:value-of select="Priority='high'"/>, '<xsl:value-of select="Wuid"/>');
                        </xsl:when>
                        <xsl:otherwise>
                            return activePopup(1, '<xsl:value-of select="$roxie"/>','<xsl:value-of select="$cluster"/>','<xsl:value-of select="$queue"/>','<xsl:value-of select="Wuid"/>',<xsl:value-of select="Priority='high'"/>, '<xsl:value-of select="Wuid"/>');
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:when>
                <xsl:when test="State='paused'">
                    return activePopup(2, '<xsl:value-of select="$roxie"/>','<xsl:value-of select="$cluster"/>','<xsl:value-of select="$queue"/>','<xsl:value-of select="Wuid"/>',<xsl:value-of select="Priority='high'"/>, '<xsl:value-of select="Wuid"/>');
                </xsl:when>
                <xsl:when test="starts-with(State,'queued')">
                    return wuidPopup('<xsl:value-of select="$roxie"/>','<xsl:value-of select="$cluster"/>','<xsl:value-of select="$queue"/>','<xsl:value-of select="Wuid"/>',<xsl:value-of select="starts-with(preceding-sibling::*[Instance=current()/Instance][position()=1]/State,'queued')"/>,<xsl:value-of select="starts-with(following-sibling::*[Instance=current()/Instance][position()=1]/State,'queued')"/>,<xsl:value-of select="Priority='high'"/>, '<xsl:value-of select="Wuid"/>');
                </xsl:when>
            </xsl:choose>
        </xsl:variable>

        <xsl:variable name="active">
            <xsl:choose>
                <xsl:when test="Priority='high'">highpriority</xsl:when>
                <xsl:when test="State='running'">active</xsl:when>
                <xsl:when test="Priority='normal'">normalpriority</xsl:when>
                <xsl:when test="Priority='low'">lowpriority</xsl:when>
                <xsl:otherwise></xsl:otherwise>
            </xsl:choose>
        </xsl:variable>

        <td class="{$active}">
            <xsl:if test="number($command) or (State='paused' and $server='ECLagent')">
                <a id="{$popupid}" class="configurecontextmenu" onclick="{$popup}">
                    &#160;
                </a>
            </xsl:if>
            <xsl:choose>
                <xsl:when test="substring(Wuid,1,1) != 'D'">
                    <a href="javascript:go('/WsWorkunits/WUInfo?Wuid={Wuid}')">
                        <xsl:choose>
                            <xsl:when test="State='running'">
                                <b>
                                    <xsl:value-of select="Wuid"/>
                                </b>
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:value-of select="Wuid"/>
                            </xsl:otherwise>
                        </xsl:choose>
                    </a>
                </xsl:when>
                <xsl:otherwise>
                    <a href="javascript:go('/FileSpray/GetDFUWorkunit?wuid={Wuid}')">
                        <xsl:choose>
                            <xsl:when test="State='running'">
                                <b>
                                    <xsl:value-of select="Wuid"/>
                                </b>
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:value-of select="Wuid"/>
                            </xsl:otherwise>
                        </xsl:choose>
                    </a>
                </xsl:otherwise>
            </xsl:choose>
        </td>
        <td class="{$active}">
            <xsl:choose>
                <xsl:when test="number(IsPausing)">
                    Pausing
                    <xsl:if test="string-length(Warning)">
                        <a class="thorstoppedrunningqueue" title="More Information" onclick="alert('{Warning}')">&#160;</a>
                    </xsl:if>
                </xsl:when>
                <xsl:when test="string-length(GraphName) and (number(MemoryBlocked) > 0)">
                    <xsl:value-of select="State"/>
                    (<a title="Graphview Control" href="javascript:go('/WsWorkunits/GVCAjaxGraph?Name={Wuid}&amp;GraphName={GraphName}&amp;SubGraphId={GID}&amp;SubGraphOnly=1')">
                        <xsl:value-of select="Duration"/>
                    </a>*)
                    <xsl:if test="string-length(Warning)">
                        <a class="thorstoppedrunningqueue" title="More Information" onclick="alert('{Warning}')">&#160;</a>
                    </xsl:if>
                </xsl:when>
                <xsl:when test="string-length(GraphName)">
                    <xsl:value-of select="State"/>
                    (<a title="Graphview Control" href="javascript:go('/WsWorkunits/GVCAjaxGraph?Name={Wuid}&amp;GraphName={GraphName}&amp;SubGraphId={GID}&amp;SubGraphOnly=1')">
                        <xsl:value-of select="Duration"/>
                    </a>)
                    <xsl:if test="string-length(Warning)">
                        <a class="thorstoppedrunningqueue" title="More Information" onclick="alert('{Warning}')">&#160;</a>
                    </xsl:if>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:value-of select="State"/>
                    <xsl:if test="string-length(Warning)">
                        <a class="thorstoppedrunningqueue" title="More Information" onclick="alert('{Warning}')">&#160;</a>
                    </xsl:if>
                </xsl:otherwise>
            </xsl:choose>
        </td>
        <td class="{$active}">
            <xsl:value-of select="Owner"/>&#160;
        </td>
        <td class="{$active}">
            <xsl:value-of select="substring(concat(substring(Jobname,1,40),'...'),1,string-length(Jobname))"/>
        </td>
    </xsl:template>

    <xsl:template match="HoleClusters">
        <h4>Hole Clusters:</h4>
        <table class="clusters" border="-1" frame="box">
            <colgroup>
                <col width="150" class="cluster"/>
                <col width="300" class="cluster"/>
            </colgroup>
            <tr>
                <th>Cluster</th>
                <th>Data Model</th>
            </tr>
            <xsl:apply-templates/>
        </table>
    </xsl:template>

    <xsl:template match="HoleCluster">
        <tr>
            <td>
                <xsl:value-of select="ClusterName"/>
            </td>
            <td>
                <xsl:value-of select="DataModel"/>
            </td>
        </tr>
    </xsl:template>

    <xsl:template match="DFUJobs">
        <h4>DFU Jobs:</h4>
        <table class="clusters" border="-1" frame="box">
            <colgroup>
                <col width="200" class="cluster"/>
                <col width="50" class="cluster"/>
                <col width="50" class="cluster"/>
                <col width="400" class="cluster"/>
            </colgroup>
            <tr>
                <th>Started</th>
                <th>Done</th>
                <th>Total</th>
                <th>Command</th>
            </tr>
            <xsl:apply-templates/>
        </table>
    </xsl:template>

    <xsl:template match="DFUJob">
        <tr>
            <td>
                <xsl:value-of select="TimeStarted"/>
            </td>
            <td>
                <xsl:value-of select="Done"/>
            </td>
            <td>
                <xsl:value-of select="Total"/>
            </td>
            <td>
                <xsl:value-of select="Command"/>
            </td>
        </tr>
    </xsl:template>

    <xsl:template match="text()|comment()"/>
</xsl:stylesheet>
