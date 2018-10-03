/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

#ifndef _WSCONFIG2_ERROR_HPP_
#define _WSCONFIG2_ERROR_HPP_

#include "errorlist.h"

#define CFGMGR_ERROR_INVALID_SESSION_ID      CONFIG_MGR_ERROR_START      // input session ID is not valid
#define CFGMGR_ERROR_MISSING_SESSION_ID      CONFIG_MGR_ERROR_START+1    // input session ID is missing
#define CFGMGR_ERROR_SESSION_NOT_LOCKED      CONFIG_MGR_ERROR_START+2    // session not locked for writing
#define CFGMGR_ERROR_SESSION_NOT_CREATED     CONFIG_MGR_ERROR_START+3    // There was an issue creating the session (see error text)
#define CFGMGR_ERROR_ENVIRONMENT_MODIFIED    CONFIG_MGR_ERROR_START+4    // The current environment has been modified
#define CFGMGR_ERROR_ENVIRONMENT_NOT_LOADED  CONFIG_MGR_ERROR_START+5    // Unable to load environment
#define CFGMGR_ERROR_LOCK_KEY_INVALID        CONFIG_MGR_ERROR_START+6    // session lock key is not valid
#define CFGMGR_ERROR_ENVIRONMENT_LOCKED      CONFIG_MGR_ERROR_START+7    // Environment is locked
#define CFGMGR_ERROR_SAVE_ENVIRONMENT        CONFIG_MGR_ERROR_START+8    // Error saving the environment
#define CFGMGR_ERROR_NO_ENVIRONMENT          CONFIG_MGR_ERROR_START+9    // No environment loaded
#define CFGMGR_ERROR_ENV_EXTERNAL_CHANGE     CONFIG_MGR_ERROR_START+10   // Environment was externally modified
#define CFGMGR_ERROR_ENVIRONMENT_LOCKING     CONFIG_MGR_ERROR_START+11   // Error locking the enironment
#define CFGMGR_ERROR_NODE_INVALID            CONFIG_MGR_ERROR_START+12   // Enironment node not valid
#define CFGMGR_ERROR_PATH_INVALID            CONFIG_MGR_ERROR_START+13   // The path specified is not valid

#endif
