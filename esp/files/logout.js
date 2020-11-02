/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.
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

function logout()
{
  var logoutRequest = new XMLHttpRequest();
  logoutRequest.onreadystatechange = function()
  { 
    if (logoutRequest.readyState != 4)
      console.log("Logout failed -- readyState: " + logoutRequest.readyState);
    else if (logoutRequest.status != 200)
      console.log("Logout failed -- status: " + logoutRequest.status);
    else
      parent.location = '/esp/files/userlogout.html';
  }
  logoutRequest.open( "GET", '/esp/logout', true );            
  logoutRequest.send( null );
}

var handleLockCallback = function(response, unlock)
{
    var errors = response.getElementsByTagName("Error");
    if (errors[0].textContent == "0") //false: no error
    {
        var obj = document.getElementById('lockDialog');
        if (obj != null)
        {
            obj.style.display = unlock ? 'none' : 'inline';
            obj.style.visibility = unlock ? 'hidden' : 'visible';
        }
    }
    else
    {
        var msgs = response.getElementsByTagName("Message");
        if (msgs.length == 0)
            alert("Unknown error");
        else
            alert("Error: " + msgs[0].textContent);
    }
}

var lockSession = function()
{
    document.getElementById('UnlockPassword').value = '';

    var lockRequest = new XMLHttpRequest();
    lockRequest.onload = function()
    {
        handleLockCallback(this.responseXML, false);
    }
    lockRequest.open('POST', "/esp/lock", true);
    lockRequest.send();
}

var enableUnlockBtn = function()
{
    document.getElementById('UnlockBtn').disabled = document.getElementById('UnlockUsername').value == '' || document.getElementById('UnlockPassword').value == '';
}

var unlockSession = function()
{
    var username = document.getElementById('UnlockUsername').value;
    var password = document.getElementById('UnlockPassword').value;
    if (username == '' || password == '')
        alert("Empty username or password not allowed");

    var unlockRequest = new XMLHttpRequest();
    unlockRequest.onload = function()
    {
        handleLockCallback(this.responseXML, true);
    }

    var url = "/esp/unlock?username=" + username + "&password=" + password;
    unlockRequest.open('POST', url, true);
    unlockRequest.send();
}

var sessionTimeout = 0;
var sessionTimer = null;
var prevReset = Date.now();
const SESSION_RESET_FREQ = 30 * 1000;

var readESPSessionTimeoutSeconds = function()
{
   var sessionTimeoutCookie = document.cookie.indexOf("ESPSessionTimeoutSeconds");
   if (sessionTimeoutCookie == -1)
       return;

   var str = document.cookie.substring(sessionTimeoutCookie + 25); //Skip "ESPSessionTimeoutSeconds="
   var pos = str.indexOf(";");
   if (pos != -1)
      str = str.substring(0, pos);
   sessionTimeout = parseInt(str);
}

var resetSessionTimer = function()
{
    if (sessionTimer)
    {
        clearTimeout(sessionTimer);
        sessionTimer = null;
    }
    if (sessionTimeout > 0)
        sessionTimer = setTimeout("logout()", sessionTimeout * 1000);
}

var sendResetSessionTimeout = function()
{
   if (Date.now() - prevReset < SESSION_RESET_FREQ)
      return;

   var resetSessionRequest = new XMLHttpRequest();
   resetSessionRequest.onreadystatechange = function()
   {
      if (resetSessionRequest.readyState == 4)
      {
         if (resetSessionRequest.status != 200)
            console.log("Reset session: HTTP error " + resetSessionRequest.status);
         else
            console.log("Session reset.");
      }
   }
   resetSessionRequest.open('POST', '/esp/reset_session_timeout', true);
   resetSessionRequest.send();
   prevReset = Date.now();
}


