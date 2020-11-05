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

#include <curl/curl.h>
#include <iostream>
#include <openssl/sha.h>
#include <string.h>

#include "jlog.hpp"

#include "nlohmann/json.hpp"

#include "jwtEndpoint.hpp"

static size_t captureIncomingCURLReply(void* contents, size_t size, size_t nmemb, void* userp)
{
    size_t          incomingDataSize = size * nmemb;
    MemoryBuffer*   mem = static_cast<MemoryBuffer*>(userp);
    size_t          MAX_BUFFER_SIZE = 4194304; // 2^22

    if ((mem->length() + incomingDataSize) < MAX_BUFFER_SIZE)
    {
        mem->append(incomingDataSize, contents);
    }
    else
    {
        // Signals an error to libcurl
        incomingDataSize = 0;

        PROGLOG("CJwtSecurityManager::captureIncomingCURLReply() exceeded buffer size %zu", MAX_BUFFER_SIZE);
    }

    return incomingDataSize;
}

static std::string hashString(const std::string& s)
{
    SHA256_CTX  context;
    char        hashedValue[SHA256_DIGEST_LENGTH];

    memset(hashedValue, 0, sizeof(hashedValue));

    if (!SHA256_Init(&context))
        throw makeStringException(-1, "CJwtSecurityManager: OpenSSL ERROR calling SHA256_Init while hashing user password");

    if (!SHA256_Update(&context, (unsigned char*)s.data(), s.size()))
        throw makeStringException(-1, "CJwtSecurityManager: OpenSSL ERROR calling SHA256_Update while hashing user password");

    if (!SHA256_Final((unsigned char*)hashedValue, &context))
        throw makeStringException(-1, "CJwtSecurityManager: OpenSSL ERROR calling SHA256_Final while hashing user password");

    return std::string(hashedValue, SHA256_DIGEST_LENGTH);
}

static void hashUserPW(const std::string& pw, const std::string& nonce, StringBuffer& encodedHash)
{
    // Add a simple salt to the user's plaintext password to protect it during
    // storage and transmission (mitigating rainbow attacks); a simple salt
    // based on the first two chars of the password is used as it is easy for
    // both the auth server and this client to agree on the salt value

    std::string     saltedPW(pw.substr(0, 2) + pw);
    std::string     firstHash(hashString(saltedPW));
    std::string     secondHash(hashString(firstHash + nonce));

    JBASE64_Encode(secondHash.data(), secondHash.size(), encodedHash, false);
}

static std::string tokenFromEndpoint(const std::string& jwtEndPoint, bool allowSelfSignedCert, const std::string& credentialsStr)
{
    std::string     apiResponse;

    // Call JWT endpoint
    CURL*   curlHandle = curl_easy_init();
    if (curlHandle)
    {
        CURLcode                curlResponseCode;
        struct curl_slist*      headers = nullptr;
        size_t                  INITIAL_BUFFER_SIZE = 32768; // 2^15
        MemoryBuffer            captureBuffer(INITIAL_BUFFER_SIZE);
        char                    curlErrBuffer[CURL_ERROR_SIZE];

        curlErrBuffer[0] = '\0';

        try
        {
            headers = curl_slist_append(headers, "Content-Type: application/json;charset=UTF-8");
            curl_easy_setopt(curlHandle, CURLOPT_HTTPHEADER, headers);

            curl_easy_setopt(curlHandle, CURLOPT_URL, jwtEndPoint.c_str());
            curl_easy_setopt(curlHandle, CURLOPT_POST, 1);
            curl_easy_setopt(curlHandle, CURLOPT_POSTFIELDS, credentialsStr.c_str());
            if (allowSelfSignedCert)
                curl_easy_setopt(curlHandle, CURLOPT_SSL_VERIFYPEER, 0);
            curl_easy_setopt(curlHandle, CURLOPT_FOLLOWLOCATION, 1);
            curl_easy_setopt(curlHandle, CURLOPT_NOPROGRESS, 1);
            curl_easy_setopt(curlHandle, CURLOPT_WRITEFUNCTION, captureIncomingCURLReply);
            curl_easy_setopt(curlHandle, CURLOPT_WRITEDATA, static_cast<void*>(&captureBuffer));
            curl_easy_setopt(curlHandle, CURLOPT_ERRORBUFFER, curlErrBuffer);

            curlResponseCode = curl_easy_perform(curlHandle);

            if (curlResponseCode == CURLE_OK && captureBuffer.length() > 0)
            {
                std::string     responseStr = std::string(captureBuffer.toByteArray(), captureBuffer.length());

                // Poor check to see if reply resembles JSON
                if (responseStr[0] == '{')
                    apiResponse = responseStr;
                else
                    PROGLOG("CJwtSecurityManager: Invalid JWT endpoint response: %s", responseStr.c_str());
            }
            else
            {
                if (curlResponseCode != CURLE_OK)
                    PROGLOG("CJwtSecurityManager login error: libcurl error (%d): %s", curlResponseCode, (curlErrBuffer[0] ? curlErrBuffer : "<unknown>"));
                else
                    PROGLOG("CJwtSecurityManager login error: No content from JWT endpoint");
            }
        }
        catch (...)
        {
            // We should not be allowing exceptions to propagate up
            // the chain; ignore the error (and let's hope the source
            // of the error logged the details)
        }

        // Cleanup
        if (headers)
        {
            curl_slist_free_all(headers);
            headers = nullptr;
        }
        curl_easy_cleanup(curlHandle);
    }

    return apiResponse;
}

std::string tokenFromLogin(const std::string& jwtEndPoint, bool allowSelfSignedCert, const std::string& userStr, const std::string& pwStr, const std::string& clientID, const std::string& nonce)
{
    nlohmann::json          credentialsJSON;
    std::string             credentialsStr;
    StringBuffer            encodedUserPW;

    hashUserPW(pwStr, nonce, encodedUserPW);

    // Construct the credentials
    credentialsJSON["username"] = userStr;
    credentialsJSON["password"] = encodedUserPW.str();
    credentialsJSON["client_id"] = clientID;
    credentialsJSON["nonce"] = nonce;
    credentialsStr = credentialsJSON.dump();

    return tokenFromEndpoint(jwtEndPoint, allowSelfSignedCert, credentialsStr);
}

std::string tokenFromRefresh(const std::string& jwtEndPoint, bool allowSelfSignedCert, const std::string& clientID, const std::string& refreshToken)
{
    nlohmann::json          credentialsJSON;
    std::string             credentialsStr;

    // Construct the credentials
    credentialsJSON["grant_type"] = "refresh_token";
    credentialsJSON["refresh_token"] = refreshToken;
    credentialsJSON["client_id"] = clientID;
    credentialsStr = credentialsJSON.dump();

    return tokenFromEndpoint(jwtEndPoint, allowSelfSignedCert, credentialsStr);
}

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    curl_global_init(CURL_GLOBAL_ALL);

    return true;
}

MODULE_EXIT()
{
    curl_global_cleanup();
}
