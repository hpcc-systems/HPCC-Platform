/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
############################################################################## */

var labelPopup = null;

function format2(n)
{
    return new String(Math.floor(n/10))+ new String(n%10);
}

// magic function from http://www.merlyn.demon.co.uk/zeller-c.htm
function ZCMJD(y, m, d) 
{
  if (m<3) { m += 12 ; y-- }
  return -678973 + d + (((153*m-2)/5)|0) + 365*y + ((y/4)|0) - ((y/100)|0) + ((y/400)|0); 
}

function formatUTC(d)
{
    var dt=new Date(d);
    if(isNaN(dt)) return null;

    return (dt.getUTCFullYear()>1950 ?  dt.getUTCFullYear() : dt.getUTCFullYear()+100)+'-'+
           format2(dt.getUTCMonth()+1)+'-'+
           format2(dt.getUTCDate())+'T'+
           format2(dt.getUTCHours())+':'+
           format2(dt.getUTCMinutes())+':'+
           format2(dt.getUTCSeconds())+'Z';
}

function formatShortDate(dt)
{  
    var re=dt.toString().match(/(\S+)\s+(\S+)\s+(\d+)/i);
    if(!re) return '';

    return re[1]+', '+re[2]+' '+re[3];
}

function parseUTC(d)
{
    var re=new String(d).match(/(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z?/);
    if (!re) return null;
    return Date.UTC(re[1],re[2]-1,re[3],re[4],re[5],re[6]);
}

function reload_graph(src)
{
    var svg0=document.getElementById('SVG0');
    //var svgdoc0=svg0.getSVGDocument();
    if (svg0 == null)
        return;

    var svgdoc0 = null;
    if (svg0.contentDocument != null) {
        svgdoc0 = svg0.contentDocument;
    }
    else if (typeof svg0.getSVGDocument != 'undefined') {
        svgdoc0 = svg0.getSVGDocument();
    }

    if (svgdoc0 == null) {
        return;
    }

    if(svgdoc0.rootElement && svgdoc0.rootElement.firstChild) {
        svgdoc0.rootElement.removeChild(svgdoc0.rootElement.firstChild);
    }

    var svg=document.getElementById('SVG');
    svg.style.width=1;
    svg.style.height=1;
    var svgdoc=svg.getSVGDocument();
    if(svgdoc.rootElement.firstChild)
        svgdoc.rootElement.removeChild(svgdoc.rootElement.firstChild);

    document.getElementById('loader').src=src;
    displayProgress('Querying dali ...');
}

function show_popup(evt,wuid,graph,started,finished,finishTime,cluster)
{
    var svg=document.getElementById('SVG');
    if(!svg) return;

    var x = evt.screenX, y = evt.screenY;
    var d=Math.floor(parseUTC(finished)-parseUTC(started))/1000,
        h=Math.floor(d/3600),
        m1=d-h*3600,
        m=Math.floor(m1/60),
        s=m1-m*60;

    var src='<table id="tab" style="font:menu"><colgroup><col align="left" valign="top"/></colgroup>'+
          '<tr><th>Wuid</th><td>'+wuid+'</td></tr>'+
          '<tr><th>Graph</th><td>'+graph+'</td></tr>'+
          '<tr><th>Cluster</th><td>'+cluster+'</td></tr>'+
          '<tr><th>Started</th><td>'+started+'</td></tr>'+
          '<tr><th>Finished</th><td>'+finishTime+'</td></tr>'+
          '<tr><th>Time</th><td>'+(h ? h+'h ':'')+(m ? m+'m ' : '')+(s +'s')+'</td></tr></table>';

    if (typeof window.createPopup != 'undefined')
    {
        var xp=x+svg.offsetLeft-document.body.scrollLeft+window.screenLeft-50,
            yp=y+svg.offsetTop-document.body.scrollTop+window.screenTop+200;

        labelPopup=window.createPopup();

        var popupBody=labelPopup.document.body;
        popupBody.style.backgroundColor = "yellow";
        popupBody.style.border = "outset black 2px";
        popupBody.innerHTML=src;
        labelPopup.show(xp,yp,400,100,null);

        var w=labelPopup.document.getElementById('tab').clientWidth+5,
            h=labelPopup.document.getElementById('tab').clientHeight+5;
        labelPopup.show(xp,yp,w,h,null);
    }
    else
    {
        var xp=x+svg.offsetLeft+document.body.scrollLeft-window.screenX-280, //- 180 - 100
            yp=y+svg.offsetTop+document.body.scrollTop-window.screenY-370; //- 250 - 120

        labelPopup = new YAHOO.widget.Panel("labelPanel", { width:"200px", height:"120px",
            visible:true, constraintoviewport:true, close:false,
            xy: [xp, yp]
             } );   
        labelPopup.setBody(src);
        labelPopup.render(document.body);
        labelPopup.show();
    }
}
                                            
function close_popup()
{
    if(labelPopup)
    {
        if (typeof labelPopup.destroy != 'undefined')
        {
            labelPopup.destroy();
        }
        else if (typeof labelPopup.close != 'undefined')
        {
            labelPopup.close();
        }
        else
            labelPopup.hide();

        labelPopup=null;
    }
}

function open_workunit(wuid)
{
    var wu_window = window.open('/WsWorkunits/WUInfo?Wuid=' + wuid,
        'Workunit', 'location=0,status=1,scrollbars=1,resizable=1,width=500,height=600');
    wu_window.opener = window;
    wu_window.focus();
}


var fromDate=null, toDate=null;
var usageArray=null;
var busagAarray = null;
var nbusageArray = null;
var totalWorkunits=0;
var showAllStat = true;
var numberOfDays = 0;

function displayProgress(status)
{
    document.getElementById('progress').innerHTML=status;
}

function initStat()
{
    usageArray=new Array(numberOfDays);
    busageArray=new Array(numberOfDays);
    nbusageArray=new Array(numberOfDays);
    for(var i=0;i<numberOfDays;i++)
    {
        usageArray[i] = 0;
        busageArray[i] = 0;
        nbusageArray[i] = 0;
    }
}

function displayBegin(from,to,showall)
{
    fromDate=new Date(parseUTC(from)), 
    toDate=new Date(parseUTC(to));

    var first=ZCMJD(fromDate.getFullYear(),fromDate.getMonth() + 1,fromDate.getDate());
    var last=ZCMJD(toDate.getFullYear(),toDate.getMonth() + 1,toDate.getDate());
    var count=last-first+1;
    numberOfDays = count;
    showAllStat = showall;
    initStat();

    totalWorkunits=0;

    var svg=document.getElementById('SVG');
    var svgdoc=svg.getSVGDocument();
    var g1=svgdoc.createElementNS("http://www.w3.org/2000/svg", "g");
    g1.setAttribute("transform","translate(120,20) scale(25,20)");
    g1.setAttribute("id","top");

    var g=svgdoc.createElementNS("http://www.w3.org/2000/svg", "g");
    g.setAttribute("stroke-width","0.01");
    g.setAttribute("stroke","black");
    g.setAttribute("font-size","0.5");
    g.setAttribute("stroke-width","0.01");
    g.setAttribute("alignment-baseline","middle");
    g.setAttribute("id","toplines");

    for(var i=0;i<=count;i++)
    {
        var today=new Date(fromDate.getFullYear(),fromDate.getMonth(),fromDate.getDate()+i);

        if(i<count)
        {
            var text1=svgdoc.createElementNS("http://www.w3.org/2000/svg", "text");
            text1.setAttribute("x",-0.5);
            text1.setAttribute("y",i+2.5);
            text1.setAttribute("text-anchor","end");
            text1.appendChild(svgdoc.createTextNode(formatShortDate(today)));
            g.appendChild(text1);

            if(today.getDay()==0 || today.getDay()==6)
            {
                var grey=svgdoc.createElementNS("http://www.w3.org/2000/svg", "line");
                grey.setAttribute("x1",0);
                grey.setAttribute("y1",i+2.5);
                grey.setAttribute("x2",24);
                grey.setAttribute("y2",i+2.5);
                grey.setAttribute("stroke","#ccc");
                grey.setAttribute("stroke-width",1);
                g.appendChild(grey);
            }
        }

        var line=svgdoc.createElementNS("http://www.w3.org/2000/svg", "line");
        line.setAttribute("x1",0);
        line.setAttribute("y1",i+2);
        line.setAttribute("x2",24);
        line.setAttribute("y2",i+2);
        if(i==0 || i==count)
        {
            line.setAttribute("stroke-width",0.05);
        }
        g.appendChild(line);
    }

    for(var i=0;i<=24;i++)
    {
        var text1=svgdoc.createElementNS("http://www.w3.org/2000/svg", "text");
        text1.setAttribute("x",i);
        text1.setAttribute("y",1.5);
        text1.setAttribute("text-anchor","middle");
        text1.appendChild(svgdoc.createTextNode(i));
        g.appendChild(text1);

        var text2=svgdoc.createElementNS("http://www.w3.org/2000/svg", "text");
        text2.setAttribute("x",i);
        text2.setAttribute("y",count+2.5);
        text2.setAttribute("text-anchor","middle");
        text2.appendChild(svgdoc.createTextNode(i));
        g.appendChild(text2);

        var line=svgdoc.createElementNS("http://www.w3.org/2000/svg", "line");
        line.setAttribute("x1",i);
        line.setAttribute("y1",2);
        line.setAttribute("x2",i);
        line.setAttribute("y2",count+2);
        if(i==0 || i==24)
        {
            line.setAttribute("stroke-width",0.05);
        }
        g.appendChild(line);
    }

    var gskew = svgdoc.createElementNS("http://www.w3.org/2000/svg", "g");
    gskew.setAttribute("transform", "translate(24.5,2) rotate(-45)");
    gskew.setAttribute("font-size","0.45");
        var text3=svgdoc.createElementNS("http://www.w3.org/2000/svg", "text");
        text3.setAttribute("x",0);
        text3.setAttribute("y",0);
        text3.setAttribute("text-anchor","start");
        text3.appendChild(svgdoc.createTextNode("OVERALL"));
        gskew.appendChild(text3);
    g.appendChild(gskew);

    var gskew2 = svgdoc.createElementNS("http://www.w3.org/2000/svg", "g");
    gskew2.setAttribute("transform", "translate(25.5,2) rotate(-45)");
    gskew2.setAttribute("font-size","0.45");
        var text4=svgdoc.createElementNS("http://www.w3.org/2000/svg", "text");
        text4.setAttribute("x",0);
        text4.setAttribute("y",0);
        text4.setAttribute("text-anchor","start");
        text4.appendChild(svgdoc.createTextNode("BUSINESS"));
        gskew2.appendChild(text4);
    g.appendChild(gskew2);

    var gskew3 = svgdoc.createElementNS("http://www.w3.org/2000/svg", "g");
    gskew3.setAttribute("transform", "translate(26.5,2) rotate(-45)");
    gskew3.setAttribute("font-size","0.45");
        var text5=svgdoc.createElementNS("http://www.w3.org/2000/svg", "text");
        text5.setAttribute("x",0);
        text5.setAttribute("y",0);
        text5.setAttribute("text-anchor","start");
        text5.appendChild(svgdoc.createTextNode("NON-BUSINESS"));
        gskew3.appendChild(text5);
    g.appendChild(gskew3);

    g1.appendChild(g);

    svgdoc.rootElement.appendChild(g1);
    svgdoc.rootElement.setAttribute("width",896);
    svgdoc.rootElement.setAttribute("height",150+20+count*20+40);
    svg.style.width=900;
    svg.style.height=150+20+count*20+40;
}

var lastjob = '';
var seq = 0;

function displayJob(wuid,graph,started,finished,cluster,state,source,showall,bbtime,betime)
{
    var first=ZCMJD(fromDate.getFullYear(),fromDate.getMonth() + 1,fromDate.getDate());

    displayProgress('Total: '+(++totalWorkunits)+' graphs');

    var from=new Date(Math.max(parseUTC(started),fromDate.getTime())),
        to=new Date(Math.min(parseUTC(finished),toDate.getTime()));
    if(from>=to) return;

    var svg=document.getElementById('SVG');
    var svgdoc=svg.getSVGDocument();

    var clr;
    if(state == 'failed')
        clr = 'red';
    else if(state == 'not finished')
        clr = 'tan';
    else if(state == 'archived')
        clr = 'black';
    else
    {
        if(wuid != lastjob)
        {
           lastjob = wuid;
           seq = !seq;
        }
        if(seq)
           clr = 'blue';
        else 
           clr = 'darkblue';
    }

    var finishTime = finished;
    if(state == 'not finished')
        finishTime = '';
        
    var g=svgdoc.createElementNS("http://www.w3.org/2000/svg", "g");
    g.setAttribute("stroke",clr);
    g.setAttribute("stroke-width",1);
    g.addEventListener("mouseover",function(evt) { show_popup(evt,wuid,graph,started,finished,finishTime,cluster); }, false);
    g.addEventListener("mouseout",function(evt) { close_popup(); }, false);
    if (source!='sasha')
        g.addEventListener("click",function(evt) { open_workunit(wuid); }, false);

    var x1=from.getHours()+from.getMinutes()/60+from.getSeconds()/3600,
        y1=ZCMJD(from.getFullYear(),from.getMonth() + 1,from.getDate())-first,
        x2=to.getHours()+to.getMinutes()/60+to.getSeconds()/3600,
        y2=ZCMJD(to.getFullYear(),to.getMonth()+1,to.getDate())-first;

    for(var y=y1;y<=y2;y++)
    {
        var xx1= (y==y1 ? x1 : 0), xx2= (y==y2 ? x2 : 24);
        var line=svgdoc.createElementNS("http://www.w3.org/2000/svg", "line");
        line.setAttribute("x1",xx1);
        line.setAttribute("y1",y+2.5);
        line.setAttribute("x2",xx2);
        line.setAttribute("y2",y+2.5);
        g.appendChild(line);

        usageArray[y]+=100*(xx2-xx1)/24;
        var bhours = ((betime < xx2)?betime:xx2) - ((bbtime > xx1)?bbtime:xx1);
        if(bhours < 0)
            bhours = 0;
        var nbhours = (xx2 - xx1 - bhours);

        if(bbtime + (24 - betime) > 0.001)
            nbusageArray[y] += 100*nbhours/(bbtime + (24 - betime));
        if(betime - bbtime > 0.001)
            busageArray[y] += 100*bhours/(betime - bbtime);
    }

    svgdoc.getElementById("top").appendChild(g);
}

function displayStat()
{
    var svg=document.getElementById('SVG');
    var svgDoc=svg.getSVGDocument();
    var topLines=svgDoc.getElementById('toplines');
    for(var i=0;i<numberOfDays;i++)
    {
        var usageStr = '';
        if(usageArray[i] < 10)
            usageStr += ' ';
        usageStr += Math.round(usageArray[i])+'%';
        if(showAllStat)
            usageStr += ' ' + Math.round(busageArray[i])+'% ' + Math.round(nbusageArray[i])+'%';

        var text=svgDoc.createElementNS("http://www.w3.org/2000/svg", "text");
        text.setAttribute("x",24.2);
        text.setAttribute("y",i+2.5);
        text.setAttribute("text-anchor","begin");
        text.appendChild(svgDoc.createTextNode(usageStr));
        topLines.appendChild(text);
    }
}

function displayEnd(xls)
{
    displayStat();
    if (totalWorkunits > 0)
        displayProgress('<table><tr><td>Total: '+(totalWorkunits)+' graphs (<a href=\"/WsWorkunits/WUClusterJobSummaryXLS?' + xls +'\">summary</a>...<a href=\"/WsWorkunits/WUClusterJobXLS?' + xls +'\">cluster_jobs.html</a>)</td></tr></table>');
    else
        displayProgress('<table><tr><td>Total: '+(totalWorkunits)+' graphs</td></tr></table>');
}

function displaySasha()
{
    displayProgress('Querying sasha ...');
}

function displayLegend()
{
    var count = 5;
    var clrArray=new Array('red', 'tan', 'black', 'blue', 'darkblue');
    var clrDiscArray=new Array('failed', 'not finished', 'archived', 'Normal', 'Normal (diffrent workunit)');

    var svg=document.getElementById('SVG0');
    var svgdoc=svg.getSVGDocument();
    var g1=svgdoc.createElementNS("http://www.w3.org/2000/svg", "g");
    g1.setAttribute("transform","translate(20,20) scale(25,20)");
    g1.setAttribute("id","LegendBaseT");

    var g=svgdoc.createElementNS("http://www.w3.org/2000/svg", "g");
    g.setAttribute("stroke-width","0.01");
    g.setAttribute("stroke","black");
    g.setAttribute("font-size","0.5");
    g.setAttribute("stroke-width","0.01");
    g.setAttribute("alignment-baseline","middle");
    g.setAttribute("id","LegendBase");
   
    for(var i=0;i<count;i++)
    {
        var line=svgdoc.createElementNS("http://www.w3.org/2000/svg", "line");
        line.setAttribute("x1",0);
        line.setAttribute("y1",i*0.5);
        line.setAttribute("x2",2);
        line.setAttribute("y2",i*0.5);
        line.setAttribute("stroke",clrArray[i]);
        line.setAttribute("stroke-width",0.5);

        g.appendChild(line);

        var text=svgdoc.createElementNS("http://www.w3.org/2000/svg", "text");
        text.setAttribute("x",2);
        text.setAttribute("y",i*0.5 + 0.2);
        text.setAttribute("text-anchor","clrDiscArray");
        text.appendChild(svgdoc.createTextNode(clrDiscArray[i]));
        g.appendChild(text);
    }
   
    g1.appendChild(g);
    svgdoc.rootElement.appendChild(g1);

    svgdoc.rootElement.setAttribute("width",396);
    svgdoc.rootElement.setAttribute("height",8+count*10+8);
    svg.style.width=400;
    svg.style.height=10+count*10+10;
}

