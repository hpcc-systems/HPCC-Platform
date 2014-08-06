<?xml version="1.0" encoding="UTF-8"?>
<!--

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
-->

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="html"/>
    <xsl:variable name="chaturl0" select="ActivityResponse/ChatURL"/>
    <xsl:variable name="sortby" select="ActivityResponse/SortBy"/>
    <xsl:variable name="descending" select="ActivityResponse/Descending"/>
    <xsl:variable name="accessRight" select="ActivityResponse/AccessRight"/>
    <xsl:variable name="countThorClusters" select="count(ActivityResponse/ThorClusterList/TargetCluster)"/>
    <xsl:variable name="countRoxieClusters" select="count(ActivityResponse/RoxieClusterList/TargetCluster)"/>
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
                    var showBannerflag='<xsl:value-of select="ShowBanner"/>';
                    var showChatURLflag='<xsl:value-of select="ShowChatURL"/>';
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

                            if (showBannerflag > 0)
                            {
                                if (document.getElementById("BannerContent") != NaN)
                                    document.getElementById("BannerContent").disabled = false;
                                if (document.getElementById("BannerColor") != NaN)
                                    document.getElementById("BannerColor").disabled = false;
                                if (document.getElementById("BannerSize") != NaN)
                                    document.getElementById("BannerSize").disabled = false;
                                if (document.getElementById("BannerScroll") != NaN)
                                    document.getElementById("BannerScroll").disabled = false;
                            }

                            if (document.getElementById("CB_BannerContent") != NaN)
                            {
                                if (showBannerflag > 0)
                                    document.getElementById("CB_BannerContent").checked = true;
                                else
                                    document.getElementById("CB_BannerContent").checked = false;
                            }

                            if ((showChatURLflag> 0) && (document.getElementById("ChatURL") != NaN))
                                document.getElementById("ChatURL").disabled = false;

                            if (document.getElementById("CB_ChatURL") != NaN)
                            {
                                if (showChatURLflag > 0)
                                    document.getElementById("CB_ChatURL").checked = true;
                                else
                                    document.getElementById("CB_ChatURL").checked = false;
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

                        function commandQueue(action,cluster,clusterType,queue,wuid,serverType,ip,port)
                        {
                            document.getElementById("ClusterType").value=clusterType;
                            document.getElementById("Cluster").value=cluster;
                            document.getElementById("QueueName").value=queue;
                            document.getElementById("Wuid").value=wuid || '';
                            document.getElementById("ServerType").value=serverType;
                            document.getElementById("NetworkAddress").value=ip;
                            document.getElementById("Port").value=port;
                            document.forms["queue"].action='/WsSMC/'+action;
                            document.forms["queue"].submit();
                        }

                        var oMenu;

                        function queuePopup(cluster,clusterType,queue,serverType,ip,port,paused,stopped,q_rowid)
                        {
                            function clearQueue()
                            {
                                if(confirm('Do you want to clear the queue for cluster: '+cluster+'?'))
                                    commandQueue("ClearQueue",cluster,clusterType,queue,'',serverType,ip,port);
                            }
                            function pauseQueue()
                            {
                                commandQueue("PauseQueue",cluster,clusterType,queue,'',serverType,ip,port);
                            }
                            function resumeQueue()
                            {
                                commandQueue("ResumeQueue",cluster,clusterType,queue,'',serverType,ip,port);
                            }
                            function showUsage()
                            {
                                document.location.href='/WsWorkunits/WUJobList?form_&Cluster='+cluster+'&Range=30';
                            }
                            var xypos = YAHOO.util.Dom.getXY(q_rowid);
                            if (oMenu) {
                                oMenu.destroy();
                            }
                            oMenu = new YAHOO.widget.Menu("activitypagemenu", {position: "dynamic", xy: xypos} );
                            oMenu.clearContent();
                            oMenu.addItems([
                                { text: "Pause", onclick: { fn: pauseQueue }, disabled: !paused && !stopped ? false : true },
                                { text: "Resume", onclick: { fn: resumeQueue }, disabled: paused && !stopped ? false : true },
                                { text: "Clear", onclick: { fn: clearQueue } }
                            ]);
                            if (clusterType == 'THOR')
                                oMenu.addItems([
                                    { text: "Usage", onclick: { fn: showUsage } }
                                ]);

                            oMenu.render("rendertarget");
                            oMenu.show();
                            return false;
                        }

                        function activeWUPopup(type, cluster,clusterType,queue,wuid,highpriority)
                        {
                            function abortWuid()
                            {
                                if(confirm('Do you want to abort '+wuid+'?'))
                                {
                                    document.location="/WsWorkunits/WUAction?ActionType=Abort&Wuids_i1="+wuid;
                                }
                            }

                            function pauseWuid()
                            {
                                if(confirm('Do you want to pause '+wuid+'?'))
                                {
                                    document.location="/WsWorkunits/WUAction?ActionType=Pause&Wuids_i1="+wuid;
                                }
                            }

                            function pauseWuidNow()
                            {
                                if(confirm('Do you want to pause '+wuid+' now?'))
                                {
                                    document.location="/WsWorkunits/WUAction?ActionType=PauseNow&Wuids_i1="+wuid;
                                }
                            }

                            function resumeWuid()
                            {
                                if(confirm('Do you want to resume '+wuid+'?'))
                                {
                                    document.location="/WsWorkunits/WUAction?ActionType=Resume&Wuids_i1="+wuid;
                                }
                            }

                            function setHighPriority()
                            {
                                commandQueue("SetJobPriority?Priority=High",cluster,clusterType,queue,wuid);
                            }

                            function setNormalPriority()
                            {
                                commandQueue("SetJobPriority?Priority=Normal",cluster,clusterType,queue,wuid);
                            }
                            var xypos = YAHOO.util.Dom.getXY(cluster + '_' + wuid);
                            if (oMenu) {
                                oMenu.destroy();
                            }

                            oMenu = new YAHOO.widget.Menu("activitypagemenu", {position: "dynamic", xy: xypos} );
                            oMenu.clearContent();
                            oMenu.addItems([
                                { text: "Abort", onclick: { fn: abortWuid } },
                                { text: "High Priority", onclick: { fn: highpriority ? setNormalPriority : setHighPriority }, checked: highpriority ? true : false }
                                ]);

                            if (type == 'LCR')
                            {
                                oMenu.addItems([
                                    { text: "Pause", onclick: { fn: pauseWuid } },
                                    { text: "PauseNow", onclick: { fn: pauseWuidNow } }
                                ]);
                            }
                            else if (type == 'paused')
                            {
                                oMenu.addItems([
                                    { text: "Resume", onclick: { fn: resumeWuid } }
                                    ]);
                            }
                            oMenu.render("rendertarget");
                            oMenu.show();
                            return false;
                        }

                        function queuedWUPopup(cluster,clusterType,queue,eclAgentQueue, wuid,prev,next,highpriority)
                        {
                            function moveupWuid()
                            {
                                commandQueue("MoveJobUp",cluster,clusterType,queue,wuid);
                            }
                            function movedownWuid()
                            {
                                commandQueue("MoveJobDown",cluster,clusterType,queue,wuid);
                            }
                            function movefrontWuid()
                            {
                                commandQueue("MoveJobFront",cluster,clusterType,queue,wuid);
                            }
                            function movebackWuid()
                            {
                                commandQueue("MoveJobBack",cluster,clusterType,queue,wuid);
                            }
                            function removeWuid()
                            {
                                if(confirm('Do you want to remove '+wuid+'?'))
                                    commandQueue("RemoveJob",cluster,clusterType,queue,wuid);
                            }
                            function setHighPriority()
                            {
                                commandQueue("SetJobPriority?Priority=High",cluster,clusterType,queue,wuid);
                            }

                            function setNormalPriority()
                            {
                                commandQueue("SetJobPriority?Priority=Normal",cluster,clusterType,queue,wuid);
                            }

                            var xypos = YAHOO.util.Dom.getXY(cluster + '_' + wuid);
                            if (oMenu) {
                                oMenu.destroy();
                            }
                            oMenu = new YAHOO.widget.Menu("activitypagemenu", {position: "dynamic", xy: xypos} );
                            oMenu.clearContent();

                            if (eclAgentQueue == '') {
                                oMenu.addItems([
                                    { text: "Move Up", onclick: { fn: moveupWuid }, disabled: prev ? false : true },
                                    { text: "Move Down", onclick: { fn: movedownWuid }, disabled: next ? false : true },
                                    { text: "Move Top", onclick: { fn: movefrontWuid }, disabled: prev ? false : true },
                                    { text: "Move Bottom", onclick: { fn: movebackWuid }, disabled: next ? false : true },
                                    { text: "Remove", onclick: { fn: removeWuid } },
                                    { text: "High Priority", onclick: { fn: highpriority ? setNormalPriority : setHighPriority }, checked: highpriority ? true : false }
                                ]);
                            } else {
                                oMenu.addItems([
                                    { text: "Remove", onclick: { fn: removeWuid } }
                                ]);
                            }
                            oMenu.render("rendertarget");
                            oMenu.show();
                            return false;
                        }

                        function chatPopup()
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
                                            <xsl:text>return chatPopup();</xsl:text>
                                        </xsl:attribute>
                                        Launch a chat window
                                    </a>
                                </td>
                            </tr>
                        </xsl:if>
                        <xsl:if test="ShowBanner = 1">
                            <tr>
                                <td>
                                    <marquee width="800" direction="left" scrollamount="{BannerScroll}" >
                                        <font color="{BannerColor}" family="Verdana" size="{BannerSize}"><xsl:value-of select="BannerContent"/></font>
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
                                    <xsl:when test="SuperUser=1">
                                        <A href="javascript:void(0)" onclick="SetBanner();">
                                            <h3>Existing Activity on Servers:</h3>
                                        </A>
                                    </xsl:when>
                                    <xsl:otherwise>
                                        <h3>Existing Activity on Servers:</h3>
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
                    <input type="hidden" name="ServerType" id="ServerType" value=""/>
                    <input type="hidden" name="NetworkAddress" id="NetworkAddress" value=""/>
                    <input type="hidden" name="Port" id="Port" value=""/>
                    <xsl:for-each select="ThorClusterList/TargetCluster">
                        <xsl:call-template name="show-queue">
                            <xsl:with-param name="workunits" select="//Running/ActiveWorkunit[(Server='ThorMaster' and TargetClusterName=current()/ClusterName) or (ClusterType='Thor' and ClusterQueueName=current()/QueueName)]"/>
                            <xsl:with-param name="cluster" select="ClusterName"/>
                            <xsl:with-param name="clusterType" select="'THOR'"/>
                            <xsl:with-param name="clusterStatus" select="ClusterStatus"/>
                            <xsl:with-param name="queue" select="QueueName"/>
                            <xsl:with-param name="queueStatus" select="QueueStatus"/>
                            <xsl:with-param name="statusDetails" select="StatusDetails"/>
                            <xsl:with-param name="warning" select="Warning"/>
                            <xsl:with-param name="thorlcr" select="ThorLCR"/>
                            <xsl:with-param name="serverType" select="'ThorMaster'"/>
                        </xsl:call-template>
                    </xsl:for-each>

                    <xsl:for-each select="RoxieClusterList/TargetCluster">
                        <xsl:call-template name="show-queue">
                            <xsl:with-param name="workunits" select="//Running/ActiveWorkunit[(Server='RoxieServer' and TargetClusterName=current()/ClusterName) or (ClusterType='Roxie' and ClusterQueueName=current()/QueueName)]"/>
                            <xsl:with-param name="cluster" select="ClusterName"/>
                            <xsl:with-param name="clusterType" select="'ROXIE'"/>
                            <xsl:with-param name="queue" select="QueueName"/>
                            <xsl:with-param name="queueStatus" select="QueueStatus"/>
                            <xsl:with-param name="clusterStatus" select="ClusterStatus"/>
                            <xsl:with-param name="statusDetails" select="StatusDetails"/>
                            <xsl:with-param name="warning" select="Warning"/>
                            <xsl:with-param name="serverType" select="'RoxieServer'"/>
                        </xsl:call-template>
                    </xsl:for-each>

                    <xsl:for-each select="HThorClusterList/TargetCluster">
                        <xsl:call-template name="show-queue">
                            <xsl:with-param name="workunits" select="//Running/ActiveWorkunit[(Server='HThorServer' and TargetClusterName=current()/ClusterName) or (ClusterType='HThor' and ClusterQueueName=current()/QueueName)]"/>
                            <xsl:with-param name="cluster" select="ClusterName"/>
                            <xsl:with-param name="clusterType" select="'HTHOR'"/>
                            <xsl:with-param name="queue" select="QueueName"/>
                            <xsl:with-param name="queueStatus" select="QueueStatus"/>
                            <xsl:with-param name="clusterStatus" select="ClusterStatus"/>
                            <xsl:with-param name="statusDetails" select="StatusDetails"/>
                            <xsl:with-param name="warning" select="Warning"/>
                            <xsl:with-param name="serverType" select="'HThorServer'"/>
                        </xsl:call-template>
                    </xsl:for-each>

                    <xsl:for-each select="ServerJobQueues/ServerJobQueue[ServerType='ECLCCserver']">
                        <xsl:call-template name="show-queue">
                            <xsl:with-param name="workunits" select="//Running/ActiveWorkunit[Server='ECLCCserver' and Instance=current()/ServerName]"/>
                            <xsl:with-param name="cluster" select="ServerName"/>
                            <xsl:with-param name="clusterType" select="ServerType"/>
                            <xsl:with-param name="queue" select="QueueName"/>
                            <xsl:with-param name="queueStatus" select="QueueStatus"/>
                            <xsl:with-param name="statusDetails" select="StatusDetails"/>
                            <xsl:with-param name="ip" select="NetworkAddress"/>
                            <xsl:with-param name="port" select="Port"/>
                            <xsl:with-param name="serverType" select="ServerType"/>
                        </xsl:call-template>
                    </xsl:for-each>

                    <xsl:for-each select="ServerJobQueues/ServerJobQueue[ServerType='ECLserver']">
                        <xsl:call-template name="show-queue">
                            <xsl:with-param name="workunits" select="//Running/ActiveWorkunit[Server='ECLserver' and Instance=current()/ServerName]"/>
                            <xsl:with-param name="cluster" select="ServerName"/>
                            <xsl:with-param name="clusterType" select="ServerType"/>
                            <xsl:with-param name="queue" select="QueueName"/>
                            <xsl:with-param name="queueStatus" select="QueueStatus"/>
                            <xsl:with-param name="statusDetails" select="StatusDetails"/>
                            <xsl:with-param name="ip" select="NetworkAddress"/>
                            <xsl:with-param name="port" select="Port"/>
                            <xsl:with-param name="serverType" select="ServerType"/>
                        </xsl:call-template>
                    </xsl:for-each>

                    <xsl:for-each select="ServerJobQueues/ServerJobQueue[ServerType='ECLAgent']">
                        <xsl:call-template name="show-queue">
                            <xsl:with-param name="workunits" select="//Running/ActiveWorkunit[Server='ECLAgent' and Instance=current()/ServerName]"/>
                            <xsl:with-param name="cluster" select="ServerName"/>
                            <xsl:with-param name="clusterType" select="ServerType"/>
                            <xsl:with-param name="queue" select="QueueName"/>
                            <xsl:with-param name="queueStatus" select="QueueStatus"/>
                            <xsl:with-param name="statusDetails" select="StatusDetails"/>
                            <xsl:with-param name="ip" select="NetworkAddress"/>
                            <xsl:with-param name="port" select="Port"/>
                            <xsl:with-param name="serverType" select="ServerType"/>
                        </xsl:call-template>
                    </xsl:for-each>

                    <xsl:for-each select="ServerJobQueues/ServerJobQueue[ServerType='DFUserver']">
                        <xsl:call-template name="show-queue">
                            <xsl:with-param name="workunits" select="//Running/ActiveWorkunit[Server='DFUserver' and QueueName=current()/QueueName]"/>
                            <xsl:with-param name="cluster" select="ServerName"/>
                            <xsl:with-param name="clusterType" select="ServerType"/>
                            <xsl:with-param name="queue" select="QueueName"/>
                            <xsl:with-param name="queueStatus" select="QueueStatus"/>
                            <xsl:with-param name="statusDetails" select="StatusDetails"/>
                            <xsl:with-param name="serverType" select="ServerType"/>
                        </xsl:call-template>
                    </xsl:for-each>
                    <xsl:apply-templates select="DFUJobs"/>
                </form>
                <xsl:if test="SuperUser=1">
                    <span id="SetBannerFrame"   style="display:none; visibility:hidden">
                        <form id="SetBannerForm">
                            <table>
                                <tr>
                                    <td valign="top">
                                        <input type="checkbox" id="CB_BannerContent" title="Display Banner:" onclick="handleSetBannerForm(0, this.checked)"/>
                                    </td>
                                    <td valign="top">Banner:</td>
                                    <td>
                                        <textarea rows="4" cols="20" style="width:520px" name="BannerContent" id="BannerContent" disabled="true"><xsl:value-of select="BannerContent"/>&#160;</textarea>
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
                                </tr>
                                <tr>
                                    <td>
                                        <input type="checkbox" id="CB_ChatURL" title="Display Chat Link:" onclick="handleSetBannerForm(1, this.checked)"/>
                                    </td>
                                    <td valign="top">Chat URL:</td>
                                    <td>
                                        <input type="text" name="ChatURL" id="ChatURL" value="{ChatURL}" size="80" disabled="true"/>
                                    </td>
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
                <div id="rendertarget" />
            </body>
        </html>
    </xsl:template>

    <xsl:template name="show-queue">
        <xsl:param name="workunits"/>
        <xsl:param name="cluster"/>
        <xsl:param name="clusterType" select="''"/>
        <xsl:param name="queue"/>
        <xsl:param name="clusterStatus" select="''"/>
        <xsl:param name="queueStatus" select="''"/>
        <xsl:param name="statusDetails" select="''"/>
        <xsl:param name="warning" select="''"/>
        <xsl:param name="thorlcr" select="'0'"/>
        <xsl:param name="serverType" select="''"/>
        <xsl:param name="ip" select="''"/>
        <xsl:param name="port" select="'0'"/>
        <xsl:variable name="showTitle">
            <xsl:choose>
                <xsl:when test="$clusterType = 'THOR'">1</xsl:when>
                <xsl:when test="($countThorClusters &lt; 1) and ($clusterType = 'ROXIE')">1</xsl:when>
                <xsl:when test="($countThorClusters &lt; 1) and ($countRoxieClusters &lt; 1) and ($clusterType = 'HTHOR')">1</xsl:when>
                <xsl:otherwise>0</xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <xsl:variable name="showWarning">
            <xsl:choose>
                <xsl:when test="$warning !=''">
                    <xsl:value-of select="$warning"/>
                </xsl:when>
                <xsl:when test="$clusterType = 'DFUserver' or $clusterType = 'ECLCCserver' or $clusterType = 'ECLagent'">
                    <xsl:choose>
                        <xsl:when test="$queueStatus='paused'"> Queue paused </xsl:when>
                        <xsl:when test="$queueStatus='stopped'"> Queue stopped </xsl:when>
                        <xsl:otherwise></xsl:otherwise>
                    </xsl:choose>
                </xsl:when>
                <xsl:otherwise></xsl:otherwise>
            </xsl:choose>
        </xsl:variable>
        <table class="clusters" border="2" frame="box" rules="groups" style="margin-bottom:5px">
            <tr>
                <xsl:variable name="pid" select="position()"/>
                <xsl:variable name="q_rowid">
                    <xsl:choose>
                        <xsl:when test="$clusterType = 'THOR'">
                            <xsl:value-of select="concat('mn_1_', $pid)"/>
                        </xsl:when>
                        <xsl:when test="$clusterType = 'ROXIE'">
                            <xsl:value-of select="concat('mn_2_', $pid)"/>
                        </xsl:when>
                        <xsl:when test="$clusterType = 'HTHOR'">
                            <xsl:value-of select="concat('mn_3_', $pid)"/>
                        </xsl:when>
                        <xsl:when test="$clusterType = 'ECLCCserver'">
                            <xsl:value-of select="concat('mn_4_', $pid)"/>
                        </xsl:when>
                        <xsl:when test="$clusterType = 'DFUserver'">
                            <xsl:value-of select="concat('mn_5_', $pid)"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:value-of select="concat('mn_6_', $pid)"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:variable>
                <td valign="top">
                    <xsl:if test="$accessRight = 'Access_Full'">
                        <xsl:variable name="popup">return queuePopup('<xsl:value-of select="$cluster"/>','<xsl:value-of select="$clusterType"/>',
                            '<xsl:value-of select="$queue"/>','<xsl:value-of select="$serverType"/>','<xsl:value-of select="$ip"/>','<xsl:value-of select="$port"/>',
                            <xsl:value-of select="$queueStatus='paused'"/>,<xsl:value-of select="$queueStatus='stopped'"/>, '<xsl:value-of select="$q_rowid"/>');
                        </xsl:variable>
                        <a id="{$q_rowid}" class="configurecontextmenu" title="Option" onclick="{$popup}">&#160;</a>
                    </xsl:if>
                    <a>
                        <xsl:choose>
                            <xsl:when test="$clusterType = 'DFUserver' or $clusterType = 'ECLCCserver' or $clusterType = 'ECLagent'">
                                <xsl:choose>
                                    <xsl:when test="$queueStatus='paused'">
                                        <xsl:attribute name="class">thorrunningpausedqueuejobs</xsl:attribute>
                                    </xsl:when>
                                    <xsl:when test="$queueStatus='stopped'">
                                        <xsl:attribute name="class">thorrunningpausedqueuejobs</xsl:attribute>
                                    </xsl:when>
                                    <xsl:otherwise>
                                        <xsl:attribute name="class">thorrunning</xsl:attribute>
                                    </xsl:otherwise>
                                </xsl:choose>
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:choose>
                                    <xsl:when test="$clusterStatus='1'">
                                        <xsl:attribute name="class">thorrunningpausedqueuejobs</xsl:attribute>
                                    </xsl:when>
                                    <xsl:when test="$clusterStatus='2'">
                                        <xsl:attribute name="class">thorrunningpausedqueuenojobs</xsl:attribute>
                                    </xsl:when>
                                    <xsl:when test="$clusterStatus='3'">
                                        <xsl:attribute name="class">thorstoppedpausedqueuenojobs</xsl:attribute>
                                    </xsl:when>
                                    <xsl:when test="$clusterStatus='4'">
                                        <xsl:attribute name="class">thorstoppedrunningqueue</xsl:attribute>
                                    </xsl:when>
                                    <xsl:otherwise>
                                        <xsl:attribute name="class">thorrunning</xsl:attribute>
                                    </xsl:otherwise>
                                </xsl:choose>
                            </xsl:otherwise>
                        </xsl:choose>
                        <xsl:attribute name="title">
                            <xsl:value-of select="$statusDetails"/>
                        </xsl:attribute>
                        <xsl:if test="$clusterType = 'THOR'">
                            <xsl:attribute name="href">javascript:go('/WsTopology/TpClusterInfo?Name=<xsl:value-of select="$cluster"/>')</xsl:attribute>
                        </xsl:if>
                        <xsl:choose>
                            <xsl:when test="$clusterType = 'ROXIE'">RoxieCluster - <xsl:value-of select="$cluster"/></xsl:when>
                            <xsl:when test="$clusterType = 'THOR'">ThorCluster - <xsl:value-of select="$cluster"/></xsl:when>
                            <xsl:when test="$clusterType = 'HTHOR'">HThorCluster - <xsl:value-of select="$cluster"/></xsl:when>
                            <xsl:when test="$clusterType = 'ECLCCserver' or $clusterType = 'ECLserver'"><xsl:value-of select="$clusterType"/> - <xsl:value-of select="$cluster"/></xsl:when>
                            <xsl:otherwise>
                                <xsl:value-of select="$clusterType"/> - <xsl:value-of select="$queue"/>
                            </xsl:otherwise>
                        </xsl:choose>
                        <xsl:if test="string-length($showWarning)">
                            <span style="background: #C00">
                                <xsl:copy-of select="$showWarning"/>
                            </span>
                        </xsl:if>
                    </a>
                </td>
            </tr>
            <tr>
                <td>
                    <table class="clusters"  border="1" frame="box" rules="all">
                        <colgroup>
                            <col width="250" class="cluster"/>
                        </colgroup>
                        <colgroup>
                            <col width="300" class="cluster"/>
                        </colgroup>
                        <colgroup>
                            <col width="150" class="cluster"/>
                        </colgroup>
                        <colgroup>
                            <col width="400" class="cluster"/>
                        </colgroup>
                        <xsl:if test="(position()=1 and $showTitle='1')">
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
                            <xsl:with-param name="clusterType" select="$clusterType"/>
                            <xsl:with-param name="queue" select="$queue"/>
                            <xsl:with-param name="thorlcr" select="$thorlcr"/>
                        </xsl:call-template>
                    </table>
                </td>
            </tr>
        </table>
    </xsl:template>

    <xsl:template name="show-queue0">
        <xsl:param name="workunits"/>
        <xsl:param name="cluster"/>
        <xsl:param name="clusterType"/>
        <xsl:param name="queue"/>
        <xsl:param name="thorlcr" select="'0'"/>

        <xsl:variable name="active1" select="$workunits[starts-with(State,'running') and $queue = QueueName]"/>
        <xsl:variable name="active2" select="$workunits[starts-with(State,'running') and $queue != QueueName]"/>
        <tbody>
            <xsl:choose>
                <xsl:when test="$workunits[1]">
                    <tr style="border:solid 2 black">
                        <xsl:if test="count($active1)">
                            <xsl:for-each select="$active1">
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
                                    <xsl:with-param name="clusterType" select="$clusterType"/>
                                    <xsl:with-param name="queue" select="$queue"/>
                                    <xsl:with-param name="thorlcr" select="$thorlcr"/>
                                </xsl:apply-templates>
                            </xsl:for-each>
                        </xsl:if>
                    </tr>

                    <xsl:if test="$clusterType='HTHOR'">
                        <tr style="border:solid 2 black">
                            <xsl:if test="count($active2)">
                                <xsl:for-each select="$active2">
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
                                        <xsl:with-param name="clusterType" select="$clusterType"/>
                                        <xsl:with-param name="queue" select="$queue"/>
                                        <xsl:with-param name="thorlcr" select="$thorlcr"/>
                                    </xsl:apply-templates>
                                </xsl:for-each>
                            </xsl:if>
                        </tr>
                    </xsl:if>

                    <xsl:for-each select="$workunits[not(starts-with(State,'running'))]">
                        <tr>
                            <xsl:apply-templates select=".">
                                <xsl:with-param name="cluster" select="$cluster"/>
                                <xsl:with-param name="clusterType" select="$clusterType"/>
                                <xsl:with-param name="queue" select="$queue"/>
                                <xsl:with-param name="thorlcr" select="$thorlcr"/>
                            </xsl:apply-templates>
                        </tr>
                    </xsl:for-each>

                    <xsl:if test="$clusterType!='HTHOR'">
                        <tr style="border:solid 2 black">
                            <xsl:if test="count($active2)">
                                <xsl:for-each select="$active2">
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
                                        <xsl:with-param name="clusterType" select="$clusterType"/>
                                        <xsl:with-param name="queue" select="$queue"/>
                                        <xsl:with-param name="thorlcr" select="$thorlcr"/>
                                    </xsl:apply-templates>
                                </xsl:for-each>
                            </xsl:if>
                        </tr>
                    </xsl:if>
                </xsl:when>
                <xsl:otherwise>
                    <td width="1100" colspan='4'>No active workunit</td>
                </xsl:otherwise>
            </xsl:choose>
        </tbody>
    </xsl:template>

    <xsl:template match="ActiveWorkunit">
        <xsl:param name="cluster" select="''"/>
        <xsl:param name="clusterType" select="''"/>
        <xsl:param name="queue" select="''"/>
        <xsl:param name="thorlcr" select="'0'"/>

        <xsl:variable name="popupid"><xsl:value-of select="$cluster"/>_<xsl:value-of select="Wuid"/></xsl:variable>
        <xsl:variable name="popup">
            <xsl:choose>
                <xsl:when test="starts-with(State,'running')">
                    <xsl:choose>
                        <xsl:when test="$thorlcr='0' or $thorlcr='noLCR'">
                            return activeWUPopup('noLCR', '<xsl:value-of select="$cluster"/>','<xsl:value-of select="$clusterType"/>',
                                '<xsl:value-of select="$queue"/>','<xsl:value-of select="Wuid"/>',<xsl:value-of select="Priority='high'"/>);
                        </xsl:when>
                        <xsl:otherwise>
                            return activeWUPopup('LCR', '<xsl:value-of select="$cluster"/>','<xsl:value-of select="$clusterType"/>',
                                '<xsl:value-of select="$queue"/>','<xsl:value-of select="Wuid"/>',<xsl:value-of select="Priority='high'"/>);
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:when>
                <xsl:when test="State='paused'">
                    return activeWUPopup('paused', '<xsl:value-of select="$cluster"/>','<xsl:value-of select="$clusterType"/>',
                        '<xsl:value-of select="$queue"/>','<xsl:value-of select="Wuid"/>',<xsl:value-of select="Priority='high'"/>);
                </xsl:when>
                <xsl:when test="starts-with(State,'queued')">
                    return queuedWUPopup('<xsl:value-of select="$cluster"/>','<xsl:value-of select="$clusterType"/>',
                        '<xsl:value-of select="$queue"/>','<xsl:value-of select="AgentQueueName"/>','<xsl:value-of select="Wuid"/>',
                        <xsl:value-of select="starts-with(preceding-sibling::*[Instance=current()/Instance][position()=1]/State,'queued')"/>,
                        <xsl:value-of select="starts-with(following-sibling::*[Instance=current()/Instance][position()=1]/State,'queued')"/>,
                        <xsl:value-of select="Priority='high'"/>);
                </xsl:when>
            </xsl:choose>
        </xsl:variable>

        <xsl:variable name="active">
            <xsl:choose>
                <xsl:when test="Priority='high'">highpriority</xsl:when>
                <xsl:when test="starts-with(State,'running')">active</xsl:when>
                <xsl:when test="Priority='normal'">normalpriority</xsl:when>
                <xsl:when test="Priority='low'">lowpriority</xsl:when>
                <xsl:otherwise></xsl:otherwise>
            </xsl:choose>
        </xsl:variable>

        <td class="{$active}">
            <xsl:if test="$accessRight='Access_Full' and $popup != ''">
                <a id="{$popupid}" class="configurecontextmenu" onclick="{$popup}">
                    &#160;
                </a>
            </xsl:if>
            <xsl:variable name="href-method">
                <xsl:choose>
                    <xsl:when test="substring(Wuid,1,1) != 'D'">/WsWorkunits/WUInfo</xsl:when>
                    <xsl:otherwise>/FileSpray/GetDFUWorkunit</xsl:otherwise>
                </xsl:choose>
            </xsl:variable>
            <xsl:variable name="href-wuidparam">
                <xsl:choose>
                    <xsl:when test="substring(Wuid,1,1) != 'D'">Wuid</xsl:when>
                    <xsl:otherwise>wuid</xsl:otherwise>
                </xsl:choose>
            </xsl:variable>
            <a href="javascript:go('{$href-method}?{$href-wuidparam}={Wuid}')">
                <xsl:choose>
                    <xsl:when test="starts-with(State,'running')">
                        <b>
                            <xsl:value-of select="Wuid"/>
                        </b>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:value-of select="Wuid"/>
                    </xsl:otherwise>
                </xsl:choose>
            </a>
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
