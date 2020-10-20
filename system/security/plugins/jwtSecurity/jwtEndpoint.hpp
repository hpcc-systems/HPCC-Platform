/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#ifndef JWTENDPOINT_HPP_
#define JWTENDPOINT_HPP_

#include <string>

/**
 * Calls the JWT login endpoint, submitting user credentials.  Expects a
 * JSON-formatted response.
 *
 * @param   jwtLoginEndpoint        Full URL to JWT login endpoint
 * @param   allowSelfSignedCert     If true, allow a self-signed certificate
 *                                  to validate https conneection; ignored
 *                                  for http connections, but you shouldn't
 *                                  be using those anyway
 * @param   userStr                 Username to validate
 * @param   pwStr                   Password to validate
 * @param   clientID                The client_id to pass to the endpoint
 * @param   nonce                   Nonce to pass to the endpoint
 *
 * @return  String containing a JSON response from the endpoint.  If an
 *          error occurs that results in something other than a JSON
 *          string then an empty string will be returned.
 */
std::string tokenFromLogin(const std::string& jwtLoginEndpoint, bool allowSelfSignedCert, const std::string& userStr, const std::string& pwStr, const std::string& clientID, const std::string& nonce);

/**
 * Calls the JWT refress endpoint, submitting a refresh token.  Expects a
 * JSON-formatted response.
 *
 * @param   jwtEndPoint             Full URL to JWT refresh endpoint
 * @param   allowSelfSignedCert     If true, allow a self-signed certificate
 *                                  to validate https conneection; ignored
 *                                  for http connections, but you shouldn't
 *                                  be using those anyway
 * @param   clientID                The client_id to pass to the endpoint
 * @param   refreshToken            Refresh token to pass to the endpoint
 *
 * @return  String containing a JSON response from the endpoint.  If an
 *          error occurs that results in something other than a JSON
 *          string then an empty string will be returned.
 */
std::string tokenFromRefresh(const std::string& jwtEndPoint, bool allowSelfSignedCert, const std::string& clientID, const std::string& refreshToken);

#endif // JWTENDPOINT_HPP_