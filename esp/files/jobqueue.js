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

var max_queue_length = 0;
/*function format2(n)
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
}*/

function reload_queue_graph(src)
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
    max_queue_length = 0;
    
    var svgdoc=svg.getSVGDocument();
    if(svgdoc.rootElement.firstChild)
        svgdoc.rootElement.removeChild(svgdoc.rootElement.firstChild);
    document.getElementById('loader').src=src;
    displayProgress('Querying dali ...');
}

function show_q_popup(x,y,wuid,dt) 
{
    var svg=document.getElementById('SVG');
    if(!svg) return;

    var src='<table id="tab" style="font:menu"><colgroup><col align="left" valign="top"/></colgroup>'+
          '<tr><th>Wuid</th><td>'+wuid+'</td></tr>'+
          '<tr><th>Time</th><td>'+dt+'</td></tr></table>';

    if (!isFF)
    {
        labelPopup=window.createPopup();
        var popupBody=labelPopup.document.body;
        popupBody.style.backgroundColor = "yellow";
        popupBody.style.border = "outset black 2px";
        popupBody.innerHTML=src;


        var xp=x+svg.offsetLeft-document.body.scrollLeft+window.screenLeft-50,
            yp=y+svg.offsetTop-document.body.scrollTop+window.screenTop+250;

        labelPopup.show(xp,yp,400,100,null);

        var w=labelPopup.document.getElementById('tab').clientWidth+5,
            h=labelPopup.document.getElementById('tab').clientHeight+5;
            
        labelPopup.show(xp,yp,w,h,null);
    }
    else
    {
        if(labelPanel)
        { 
            labelPanel.hide();
            labelPanel=null;
        }

        var l = x-1550;
        var t = y-350;
        labelPanel = new YAHOO.widget.Panel("labelPanel", { width:"160px", height:"60px", 
            visible:true, constraintoviewport:true, close:false,
            xy: [l, t] 
             } );   
        //labelPanel.setHeader("WUInfo");   
        labelPanel.setBody(src);   
        labelPanel.render(document.body);  
        labelPanel.show(); 
    }
}

function show_q_popup1(evt,wuid,dt) 
{
    show_q_popup(evt.screenX,evt.screenY,wuid,dt);
}
/*
function hide_popup()
{
    if(window.labelPopup)
    {
        labelPopup.hide();
        labelPopup=null;
    }
}

function open_workunit(wuid)
{
   //document.location.href='/WsWorkunits/WUInfo?Wuid='+wuid;
    var wu_window = window.open('/WsWorkunits/WUInfo?Wuid=' + wuid,
        'Workunit', 'location=0,status=1,scrollbars=1,resizable=1,width=500,height=600');
    wu_window.opener = window;
    wu_window.focus();
}
*/

var fromDate=null, toDate=null;
var usageArray=null;
var busagAarray = null;
var nbusageArray = null;
var totalWorkunits=0;
var itemWidth=0.4;
function displayQLegend()
{
    var count = 3;
    var clrArray=new Array('green', 'blue', 'red');
    var clrDiscArray=new Array('Connected', 'Running', 'Queued');

    var svg=document.getElementById('SVG0');
    var svgdoc=svg.getSVGDocument();
    var g1=svgdoc.createElementNS("http://www.w3.org/2000/svg", "g");
    g1.setAttribute("transform","translate(20,20) scale(25,20)");

    var g=svgdoc.createElementNS("http://www.w3.org/2000/svg", "g");
    g.setAttribute("stroke-width","0.01");
    g.setAttribute("stroke","black");
    g.setAttribute("font-size","0.5");
    g.setAttribute("stroke-width","0.01");
    g.setAttribute("alignment-baseline","middle");

    for(var i=0;i<count;i++)
    {
        var line=svgdoc.createElementNS("http://www.w3.org/2000/svg", "line");
        line.setAttribute("x1",0);
        line.setAttribute("y1",i*0.5);
        line.setAttribute("x2",1);
        line.setAttribute("y2",i*0.5);
        line.setAttribute("stroke",clrArray[i]);
        line.setAttribute("stroke-width",0.5);
        g.appendChild(line);

        var text=svgdoc.createElementNS("http://www.w3.org/2000/svg", "text");
        text.setAttribute("x",1.5);
        text.setAttribute("y",i*0.5 + 0.2);
        text.setAttribute("text-anchor","clrDiscArray");
        text.appendChild(svgdoc.createTextNode(clrDiscArray[i]));
        g.appendChild(text);
    }

    g1.appendChild(g);
    svgdoc.rootElement.appendChild(g1);
    svg.style.width=400;
    svg.style.height=10+count*10+10;
    svgdoc.rootElement.setAttribute("width",396);
    svgdoc.rootElement.setAttribute("height",8+count*10+8);
}

var max_queues = 0;
var records = 0;
function displayQBegin(queues, connected, count)
{
    max_queues=queues;
     records = count;

    var svg=document.getElementById('SVG');
    var svgdoc=svg.getSVGDocument();
    var g1=svgdoc.createElementNS("http://www.w3.org/2000/svg", "g");
    g1.setAttribute("transform","translate(60,20) scale(25,20)");
    g1.setAttribute("id","top");

    var g=svgdoc.createElementNS("http://www.w3.org/2000/svg", "g");
    g.setAttribute("stroke-width","0.01");
    g.setAttribute("stroke","black");
    g.setAttribute("font-size","0.5");
    g.setAttribute("stroke-width","0.01");
    g.setAttribute("alignment-baseline","middle");

     var lines = queues + connected + 1;
    for(var i=0;i<lines;i++)
    {
        var text1=svgdoc.createElementNS("http://www.w3.org/2000/svg", "text");
         text1.setAttribute("x",-0.5);
         text1.setAttribute("y",i);
         text1.setAttribute("text-anchor","end");
         text1.appendChild(svgdoc.createTextNode(lines - i - 1));
         g.appendChild(text1);

        var line=svgdoc.createElementNS("http://www.w3.org/2000/svg", "line");
        line.setAttribute("x1",0);
        line.setAttribute("y1",i);
        line.setAttribute("x2",count*itemWidth+0.3);
        line.setAttribute("y2",i);
        if(i==0 || i==(lines-1))
        {
            line.setAttribute("stroke-width",0.05);
        }
        g.appendChild(line);
    }

     var line1=svgdoc.createElementNS("http://www.w3.org/2000/svg", "line");
     line1.setAttribute("x1",0);
     line1.setAttribute("y1",0);
     line1.setAttribute("x2",0);
     line1.setAttribute("y2", lines - 1);
     line1.setAttribute("stroke-width",0.05);
     g.appendChild(line1);

    g1.appendChild(g);

    svgdoc.rootElement.appendChild(g1);
    svg.style.width=1150;
    svg.style.height=150+20+count*20+40;
    svgdoc.rootElement.setAttribute("width",1148);
    svgdoc.rootElement.setAttribute("height",20+count*20+40);
}

var lastjob = '';
var seq = 0;

function displayQueue(count,dt,running,queued,waiting,connected,idle,wuid1, wuid2)
{
   var svg=document.getElementById('SVG');
   var svgdoc=svg.getSVGDocument();

    var g=svgdoc.createElementNS("http://www.w3.org/2000/svg", "g");
    g.setAttribute("stroke","black");
    g.setAttribute("font-size","0.4");
    g.setAttribute("stroke-width","0.02");
    g.setAttribute("alignment-baseline","middle");
 
    var xpos = itemWidth*count - 0.1;
    var ybase = max_queues + 2;
    var ypos = ybase+0.2;

    if (dt != '')
    {
        var gskew = svgdoc.createElementNS("http://www.w3.org/2000/svg", "g");
        var trans = "translate(" + xpos + "," + ypos + ") rotate(-90)";
        gskew.setAttribute("transform", trans);
        var text3=svgdoc.createElementNS("http://www.w3.org/2000/svg", "text");
        text3.setAttribute("x",0);
        text3.setAttribute("y",0);
        text3.setAttribute("text-anchor","end");
        text3.appendChild(svgdoc.createTextNode(dt));
        gskew.appendChild(text3);
        g.appendChild(gskew);
    }

    if (connected > 0)
    {
        var line1=svgdoc.createElementNS("http://www.w3.org/2000/svg", "line");
        line1.setAttribute("stroke",'green');
        line1.setAttribute("x1",xpos-0.15);
        line1.setAttribute("y1",ybase);
        line1.setAttribute("x2",xpos-0.15);
        line1.setAttribute("y2",ybase - connected);
        line1.setAttribute("stroke-width",0.15);
       g.appendChild(line1);
    }

    if (running > 0)
    {
        var g1=svgdoc.createElementNS("http://www.w3.org/2000/svg", "g");
        g1.setAttribute("stroke","blue");
        g1.setAttribute("stroke-width",1);

        g1.addEventListener("mouseover",function(evt) { show_q_popup1(evt,wuid1,dt); }, false);
        g1.addEventListener("mouseout",function(evt) { hide_popup(); }, false);
        g1.addEventListener("click",function(evt) { open_workunit(wuid1); }, false);

        var line2=svgdoc.createElementNS("http://www.w3.org/2000/svg", "line");
        line2.setAttribute("x1",+xpos);
        line2.setAttribute("y1",ybase);
        line2.setAttribute("x2",xpos);
        line2.setAttribute("y2",ybase - 1);
        line2.setAttribute("stroke-width",0.15);
        g1.appendChild(line2);

        g.appendChild(g1);
    }

    if (running > 1)
    {
        var g1=svgdoc.createElementNS("http://www.w3.org/2000/svg", "g");
        g1.setAttribute("stroke","blue");
        g1.setAttribute("stroke-width",1);

        g1.addEventListener("mouseover",function(evt) { show_q_popup1(evt,wuid2,dt); }, false);
        g1.addEventListener("mouseout",function(evt) { hide_popup(); }, false);
        g1.addEventListener("click",function(evt) { open_workunit(wuid2); }, false);

        var line2=svgdoc.createElementNS("http://www.w3.org/2000/svg", "line");
        line2.setAttribute("x1",+xpos);
        line2.setAttribute("y1",ybase - 1);
        line2.setAttribute("x2",xpos);
        line2.setAttribute("y2",ybase - 2);
        line2.setAttribute("stroke-width",0.15);
        g1.appendChild(line2);

        g.appendChild(g1);
    }

    if (queued > 0)
    {
        var line1=svgdoc.createElementNS("http://www.w3.org/2000/svg", "line");
        line1.setAttribute("stroke",'red');
        line1.setAttribute("x1",xpos);
        line1.setAttribute("y1",ybase - running);
        line1.setAttribute("x2",xpos);
        line1.setAttribute("y2",ybase - running - queued);
        line1.setAttribute("stroke-width",0.15);
       g.appendChild(line1);
    }

    if (queued > max_queue_length)
        max_queue_length = queued;
   svgdoc.getElementById("top").appendChild(g);
}

function displayQEnd(msg)
{
    //displayProgress('<table><tr><td>Total: '+(max_queues)+' graphs (<a href=\"/WsWorkunits/WUClusterJobXLS?' + xls +'\">xls</a>...<a href=\"/WsWorkunits/WUClusterJobSummaryXLS?' + xls +'\">summary</a>)</td></tr></table>');
    displayProgress(msg);
    
    var svg=document.getElementById('SVG');
    var svgdoc=svg.getSVGDocument();
    svg.style.width=1150;
    svg.style.height=210+max_queue_length*20;
    svgdoc.rootElement.setAttribute("width",1148);
    svgdoc.rootElement.setAttribute("height",210+max_queue_length*20);
}


