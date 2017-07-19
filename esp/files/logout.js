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
