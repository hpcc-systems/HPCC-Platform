/*##############################################################################

## Copyright © 2011 HPCC Systems.  All rights reserved.
############################################################################## */

function onHint(name, value)
{
    var txtarea = document.getElementsByName(name)[0];
    if (txtarea)
    {
        txtarea.value = value;
    }
}
function onEmpty(name)
{
    var txtarea = document.getElementsByName(name)[0];
    if (txtarea)
    {
        txtarea.value = "";
    }    
}