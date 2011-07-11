/*##############################################################################

## Copyright © 2011 HPCC Systems.  All rights reserved.
############################################################################## */

var tooltipDiv = null;
var tooltipPopup = null;
var tooltipX = 0;
var tooltipY = 0;
var tooltipSrcObj = null;
var tooltipCaptionColor = "#6699FF"; //"#808080";
var tooltipBodyColor = "#FFFF99"; //"#C0C0C0";
var tooltipBodyTextColor = "black";
var tooltipMaxWidth = screen.width;

function EnterContent(tooltipDivId, TTitle, TContent, nowrap, align) 
{
    var div = document.all[tooltipDivId];
    if (!div)
        return;
        
   var s = [];
   s.push('<table border="0" cellspacing="0" cellpadding="2px 0px 2px 0px" style="font:10pt serif; text-align:center; vertical-align:center"');
   if (nowrap)
      s.push(' width="100%"');
   s.push('>');
   
   if (TTitle)
     s.push('<tr><td ' + (nowrap ? 'nowrap="true"' : '') + 
            ' bgcolor="'+tooltipCaptionColor+'">'+ TTitle+'</td></tr>');
   
    if (!align)
        align = 'left';

   s.push( '<tr><td align="' + align + '" ' + (nowrap ? 'nowrap="true"' : '') + 
           ' bgcolor="'+tooltipBodyColor+'" style="color:' + tooltipBodyTextColor + '">'+
             TContent.replace(new RegExp('\n', 'g'), '<br/>')+ 
             '</td></tr></table>' );
   
    tooltipDiv = div;
    div.style.width = 0;//reset width
    div.innerHTML = s.join('');
    
    //if the tooltip is too wide then limit it to width defined by tooltipMaxWidth
    //
    if (tooltipDiv.offsetWidth > tooltipMaxWidth && nowrap)
        EnterContent(tooltipDivId, TTitle, TContent, false);
}

function tooltipContextMenu()
{
   var srcObj = tooltipSrcObj;
    deActivate();
    return srcObj ? srcObj.oncontextmenu() : true;
}

function tooltipClick()
{
   var srcObj = tooltipSrcObj;
    deActivate();
    return srcObj ? srcObj.onclick() : true;
}

function Activate(object, x, y)
{
    if (tooltipDiv)
    {
        tooltipPopup=window.createPopup();
        var tooltipBody=tooltipPopup.document.body;
        //tooltipBody.style.backgroundColor = "yellow";
        tooltipBody.style.border = "outset black 1px";
        tooltipBody.innerHTML=tooltipDiv.innerHTML;
        
        if (object)
        {
            tooltipBody.oncontextmenu = tooltipContextMenu;
            tooltipBody.onclick = tooltipClick;
        }
        
        overhere(object, x, y);
    }
}

function overhere(object, x, y) 
{
   if (tooltipPopup && tooltipDiv)
   {
     if (object == null)
     {
             var e = window.event;
             if (e == null)
                 return;
                 
        x = e.screenX;
        y = e.screenY;
        object = tooltipSrcObj;
         }
                 
      if (x != tooltipX || y != tooltipY || object != tooltipSrcObj)
       {
        tooltipX = x;
        tooltipY = y;
        tooltipSrcObj = object;
            tooltipPopup.show(tooltipX+5, tooltipY+5, tooltipDiv.offsetWidth+2, tooltipDiv.offsetHeight, object);
       }
    }
}

function deActivate()
{
   if (tooltipPopup)
   {
       tooltipPopup.hide();
       tooltipPopup=null;
       tooltipX = tooltipY = 0;
       tooltipSrcObj = null;
       tooltipDiv = null;
   }
}
