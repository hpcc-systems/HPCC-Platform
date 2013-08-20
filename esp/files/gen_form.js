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

// for test purpose
function getAttributes(ctrl)
{ 
    var results = "";
    var attrs = ctrl.attributes; 
    for (var i = 0; i < attrs.length; i++) { 
       var attr = attrs[i];
       results += attr.nodeName + '=' + attr.nodeValue + ' (' + attr.specified + ')<BR>'; 
    }
    return results;
} 

//==================================================================
// Restore dynamically generate content, and user input values(non-IE browsers only)

function restoreDataFromCache() 
{
    var vals = document.getElementById("esp_vals_").value;
    if (vals && vals!="")
    {
        // alert("esp_vals_ = "+vals);
        var end = vals.indexOf('|');
        while (end>0)
        {  
            var name = vals.substring(0,end);
            vals = vals.substring(end+1);
            end = vals.indexOf("|");
            if (end<0) break;
            var ctrl = document.getElementsByName(name)[0];
            if (ctrl) 
            {
                if (ctrl.type == 'checkbox')
                {
                    ctrl.checked = vals.substring(0,end)=='1';
                    //  alert("Name = "+name+", string = "+ vals.substring(0,end) + ", ctrl.checked = "+ ctrl.checked);
                }
                else if (ctrl.type == 'text' || ctrl.type == 'textarea')
                    ctrl.value = decodeURI(vals.substring(0,end));
                else if(ctrl.type == 'radio')
                {
                    //alert("Name = "+name+", string = "+ vals.substring(0,end) + ", ctrl.checked = "+ ctrl.checked);
                    if(vals.substring(0,end) == '0')
                    {
                         ctrl = document.getElementsByName(name)[1];
                    }

                    if(ctrl)                               
                        ctrl.checked = true;
                }
                else if(ctrl.type == 'select-one') 
            {
                    //alert("Select: name="+ctrl.name+"; value="+vals.substring(0,end));
                    ctrl.options[vals.substring(0,end)].selected = true;
                }
                //TODO: more types
            }
            vals = vals.substring(end+1);
            end = vals.indexOf("|");
        }
    }
}

function disableAllInputs(self)
{
    var toEnable = self.checked ? 1 : 0;
    var form = document.forms['esp_form'];  
    var ctrls = form.elements;  
    for (var idx=0; idx<ctrls.length; idx++)
    {
        var c = ctrls[idx];
        if ( (c.id!='') && (c.id.substr(0,3) == '$V.')) 
        {
            if ( (c.checked && !toEnable) || (!c.checked && toEnable))
                c.click(); 
        }
    }   
}

function disableInputControls(form)
{
    var ctrls = form.elements;  
    for (var idx=0; idx<ctrls.length; idx++)
    {
        var c = ctrls[idx];
        if ( (c.id!='') && (c.id.substr(0,3) == '$V.') && !c.checked)
        {
             var id = c.id.substring(3);
             var ctrl = document.getElementById(id);
             if (ctrl) // struct name has no id
                disableInputControl(ctrl,true);
             var label = document.getElementById('$L.'+id)
             disableInputLabel(label,true);
        }
    }
}


function onPageLoad() 
{   
   var ctrl = document.getElementById('esp_html_');
   //alert("onPageLoad(): ctrl="+ctrl+"; length="+ctrl.value.length+";value='"+ctrl.value+"'");
   if (ctrl && ctrl.value != undefined && ctrl.value!="")
   {
      //alert("Restore ctrol value: " + ctrl.value);
        document.forms['esp_form'].innerHTML = ctrl.value;
   }
  
   var form = document.forms['esp_form'];
   initFormValues(form, getUrlFormValues(top.location.href));

   disableInputControls(form);     

   // IE seems need this now too
   //if (isIE) return true;
   // FF 1.5 history cache works, but seems to stop working afterwards
   restoreDataFromCache();    
             
   return true;
}

function getUrlFormValues(url)
{
    var idx = url.indexOf('?');
    if (idx>0) 
        url = url.substring(idx+1);
    var a = url.split('&');

    var ps = new Hashtable();
    for (var i=0; i<a.length; i++)
    {
         idx = a[i].indexOf('=');
         if (a[i].charAt(0) == '.' && idx>0)
         {
            var key = a[i].substring(0,idx);
            var val =  a[i].substring(idx+1);
            if (val != '')
                ps.put(key, val);
        } 
    }

    return ps;
}

function getUrlEspFlags(url)
{
    var idx = url.indexOf('?');
    if (idx>0) 
        url = url.substring(idx+1);
    var a = url.split('&');

    var ps = new Hashtable();
    for (var i=0; i<a.length; i++)
    {
        if (a[i].charAt(0) != '.') 
        {
            idx = a[i].indexOf('=');
            if (idx>0) 
            {
                var key = a[i].substring(0,idx);
                var val =   a[i].substring(idx+1);
                ps.put(key,val);
            } else
                ps.put(a[i],"");
        }
    }

    return ps;
}

function createArray(ps)
{
    var remains = new Hashtable();
    ps.moveFirst();
    while (ps.next())
    {
         var name = ps.getKey();
         var val = ps.getValue();
         // alert(name + ": " + val);
         if (val > 0 && name.substring(name.length-10)==".itemcount")
         {
             var id = name.substring(1, name.length-10) + '_AddBtn';
             var ctrl = document.getElementById(id);
             if (ctrl)
             {                       
                //   alert("name: " + ctrl.tagName + ", type " + ctrl.type);
                for (var i=0; i<val; i++)
                    ctrl.click();
             }
             else {
                //alert("Can not find control: " + id);
                remains.put(name,val);
             }
         }
    }

    if (remains.size()>0)
      return remains;
    else
      return null;
}
  
function initFormValues(form, ps)
{      
    // create array controls
    // Implementation NOTE: The Add order is important: if array A contains array B, item in A must be created first before B can be created.

    var working = ps;
    do
    {
        working  = createArray(working);
        //alert("Left: " + working);
    } while (working!=null);
    
    // init values
    ps.moveFirst();
    while (ps.next()) 
    {
        var name = ps.getKey();
        if (name.charAt(0) != '.')
        {
            //alert("Skip " + name);
            continue;
        }
        name = name.substring(1);              
        var val = ps.getValue();               
        ctrl = document.getElementsByName(name)[0];

        // alert("Set value for " + name + ": " + val + ". Ctrl type: " + ctrl.type);
        if (ctrl) 
        {
            if (ctrl.type == 'checkbox') {
                ctrl.checked = val =='1';
            }
            else if (ctrl.type == 'text') {
                ctrl.value =  decodeURIComponent(val); //    decodeURI(vals.substring(0,end)); //TODO: do we need encoding            
            }
            else if (ctrl.type == 'textarea') {           
                ctrl.value =  decodeURIComponent(val); 
            }
            else if(ctrl.type == 'radio')  {
                if(val == '0')
                    ctrl = document.getElementsByName(name)[1];
                if(ctrl)                                
                    ctrl.checked = true;
            }
            else if (ctrl.type=='select-one')  {
                //alert("Set select value: " + val);
                ctrl.options[val].selected=true;
            }
        }
        else
            alert("failed to find contrl: " + name);
    }
}
  
function doBookmark(form)
{
    var ps = getUrlEspFlags(form.action); 

    var ctrls = form.elements;  
    for (var idx=0; idx<ctrls.length; idx++)
    {
        var c = ctrls[idx];
        if ( (c.name!='') && (c.value != '') )
        {
            if (c.tagName == 'TEXTAREA') {
                ps.put('.'+c.name,encodeURIComponent(c.value));
            } else if (c.tagName == "SELECT") {
                ps.put('.'+c.name, c.selectedIndex); // use the index
            } else if (c.tagName == 'INPUT')  {
                if ( c.type == 'text' || c.type=='password')  {
                    ps.put('.'+c.name,encodeURIComponent(c.value)); // existing one is overwrotten
                } else if (c.type == 'radio' && c.checked) {
                    if (c.id.substring(c.id.length-5) == '.true')
                        ps.put('.'+c.name,"1");
                    else if (c.id.substring(c.id.length-6) == '.false')
                        ps.put('.'+c.name,"0");
                } else if ( c.type=='hidden') { 
                    // alert("hidden:"+c.name+", value " + c.value + ", sub = " +  c.name.substring(c.name.length-10));
                    if (c.value!='0' && c.name.substring(c.name.length-10)=='.itemcount') 
                        ps.put('.'+c.name,c.value);
                }
            }
        }
    }

    var idx = form.action.indexOf('?');
    var action = (idx>0) ? form.action.substring(0,idx) : form.action;

    action += "?form"; 

    var parm = "";
    ps.moveFirst();
    while (ps.next()) 
       parm += '&' + ps.getKey() + '=' + ps.getValue();
    //alert("parm="+parm);
    
    /*  
    // TODO: make inner frame work
    var url = "/?inner=.." + path + "%3Fform";
    top.location.href = url + parm;
    */       
    top.location.href = action + parm;
}

//==================================================================
// Save dynamically generate content, and user input values(non-IE browsers only)
function onSubmit(reqType)  // reqType: 0: regular form, 1: soap, 2: form param passing
{
    var form = document.forms['esp_form'];
    if (!form)  return false;

    // remove "soap_builder_" (somehow FF (not IE) remembers this changed form.action )
    if (reqType != 1)
    {
        var action = form.action;
        var idx = action.indexOf('soap_builder_');
        if (idx>0) 
        {
            if (action.length <= idx + 13) // no more char after 'soap_builder_'
            {
                var ch = action.charAt(idx-1);
                if (ch == '&' || ch == '?')                     
                    action = action.substring(0,idx-1);
            } else {
                var ch = action.charAt(idx+13) // the char after 'soap_builder_';
                if (ch == '&')
                   action = action.substring(0,idx) + action.substring(idx+13);
            }       
        
            // alert("Old action: " + form.action + "\nNew action: " + action);
            form.action = action;
        }
    }

    // --  change action if user wants to
    var dest = document.getElementById('esp_dest');
    if (dest && dest.checked)
    {
         form.action = document.getElementById('dest_url').value;
    }      
    if (reqType==1)
    {
         if (form.action.indexOf('soap_builder_')<0) // add only if does not exist already
         {
                var c =  (form.action.indexOf('?')>0) ? '&' : '?';
                form.action += c + "soap_builder_";
         }
    }
    else if (reqType==2) 
    {
         doBookmark(form);
    }
    if (reqType==3)
    {
         if (form.action.indexOf('roxie_builder_')<0) // add only if does not exist already
         {
                var c =  (form.action.indexOf('?')>0) ? '&' : '?';
                form.action += c + "roxie_builder_";
         }
    }
    // alert("Form action = " + form.action);

    // firefox now save input values (version 1.5)  
    saveInputValues(form);

    return true;
}

//==================================================================
// Save dynamically generate content, and user input values(non-IE browsers only)
function onWsEcl2Submit(path)  // reqType: 0: regular form, 1: soap, 2: form param passing, 3: roxiexml
{
    var form = document.forms['esp_form'];
    if (!form)  return false;

    var dest = document.getElementById('esp_dest');
    if (dest && dest.checked)
    {
         form.action = document.getElementById('dest_url').value;
    }      
    else if (path=="bookmark")
    {
         doBookmark(form);
    }
    else
    {
        form.action = path;
    }
    
    alert("Form action = " + form.action);

    // firefox now save input values (version 1.5)  
    saveInputValues(form);

    return true;
}


function saveInputValues(form)
{ 
    // -- save values in input for browser
    var ctrl = document.getElementById('esp_html_');
    // IE seems to need this too
    //if (isIE || !ctrl)  return true;

    ctrl.value=form.innerHTML;          

    // save all user input
    var ctrls = form.elements;
    var items = ctrls.length;      
    var inputValues = "";

    for (var idx=0; idx<ctrls.length; idx++)
    {
        var item = ctrls[idx];
        var name = item.name;

        if (!name) continue;

        //NOTE: we can not omit empty value since it can be different from the default
        if (item.type == 'checkbox')
            inputValues += name+"|"+(item.checked ? "1" : "0")+"|";
        else if (item.type =='text' || item.type=='textarea')
        {   
           inputValues += name+"|" + encodeURI(item.value) +"|";
           //alert("value added: "+inputValues[inputValues.length-1] +", input items: " + inputValues.length+", values="+inputValues.toString());
        }
        else if (item.type == 'radio')
        {
            //if(item.checked) // the unchecked value can be different from the default
            inputValues += name+"|"+item.value+"|";
        }
        else if (item.type == 'select-one')
        {
        inputValues += name+"|"+item.selectedIndex+"|";
        //alert("inputValues=" + inputValues + "; index="+item.selectedIndex);
        }
        // TODO: other control types
    }

    document.getElementById("esp_vals_").value = inputValues;
}

//==================================================================
// Reset all values to orginal (all dynamically generated arrays are removed)
function onClearAll()
{
    // reset dynamic generated content
    var reqCtrl = document.getElementById('esp_dyn');
    reqCtrl.innerHTML = getRequestFormHtml();;
 
    // clear cache
    var ctrl = document.getElementById('esp_html_');
    if (ctrl) 
        ctrl.value = "";
    ctrl = document.getElementById("esp_vals_");
    if (ctrl)
        ctrl.value = "";
}

//  Exclusive selectable
function  onClickSort(chked)
{
    if (chked) {
         document.getElementById("esp_validate").checked = false;
    }
}

function  onClickValidate(chked)
{
    if (chked) {
        document.getElementById("esp_sort_result").checked = false;        
    }
}
