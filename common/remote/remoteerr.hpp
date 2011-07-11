/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#ifndef REMOTEERR_HPP
#define REMOTEERR_HPP

#define ERR_REMOTE_FIRST                        8000
#define ERR_REMOTE_LAST                         8049

#define RFSERR_InvalidCommand                   8000
#define RFSERR_NullFileIOHandle                 8001
#define RFSERR_InvalidFileIOHandle              8002
#define RFSERR_TimeoutFileIOHandle              8003
#define RFSERR_OpenFailed                       8004
#define RFSERR_ReadFailed                       8005
#define RFSERR_WriteFailed                      8006
#define RFSERR_RenameFailed                     8007
#define RFSERR_ExistsFailed                     8009
#define RFSERR_RemoveFailed                     8010
#define RFSERR_CloseFailed                      8011
#define RFSERR_IsFileFailed                     8012
#define RFSERR_IsDirectoryFailed                8013
#define RFSERR_IsReadOnlyFailed                 8014
#define RFSERR_SetReadOnlyFailed                8015
#define RFSERR_GetTimeFailed                    8016
#define RFSERR_SetTimeFailed                    8017
#define RFSERR_CreateDirFailed                  8018
#define RFSERR_GetDirFailed                     8019
#define RFSERR_GetCrcFailed                     8020
#define RFSERR_MoveFailed                       8021
#define RFSERR_ExtractBlobElementsFailed        8022
#define RFSERR_CopyFailed                       8023
#define RFSERR_AppendFailed                     8024
#define RFSERR_AuthenticateFailed               8025
#define RFSERR_CopySectionFailed                8026
#define RFSERR_TreeCopyFailed                   8027


#define RAERR_InvalidUsernamePassword           8040
#define RFSERR_MasterSeemsToHaveDied            8041
#define RFSERR_TimeoutWaitSlave                 8042
#define RFSERR_TimeoutWaitConnect               8043
#define RFSERR_TimeoutWaitMaster                8044
#define RFSERR_NoConnectSlave                   8045
#define RFSERR_NoConnectSlaveXY                 8046
#define RFSERR_VersionMismatch                  8047

//---- Text for all errors (make it easy to internationalise) ---------------------------

#define RFSERR_InvalidCommand_Text              "Unrecognised command %d"
#define RFSERR_NullFileIOHandle_Text            "Remote file operation on NULL fileio"
#define RFSERR_InvalidFileIOHandle_Text         "Remote file operation on invalid fileio"
#define RFSERR_TimeoutFileIOHandle_Text         "Remote fileio has been closed because of timeout"
#define RFSERR_MasterSeemsToHaveDied_Text       "Master program seems to have died..."
#define RFSERR_VersionMismatch_Text             "Slave version does not match, expected %d got %d"

#define RFSERR_TimeoutWaitSlave_Text            "Timeout waiting for slave %s to respond"
#define RFSERR_TimeoutWaitConnect_Text          "Timeout waiting to connect to slave %s"
#define RFSERR_TimeoutWaitMaster_Text           "Timeout waiting to connect to master"
#define RFSERR_NoConnectSlave_Text              "Failed to start slave program"
#define RFSERR_NoConnectSlaveXY_Text            "Failed to start slave program %s on %s"

#define RAERR_InvalidUsernamePassword_Text      "Invalid (upper case U) in username/password"


interface IDAFS_Exception: extends IException
{ // Raise by dafilesrv calls
};

enum DAFS_ERROR_CODES {
    DAFSERR_connection_failed               = -1,   
    DAFSERR_authenticate_failed             = -2,
    DAFSERR_protocol_failure                = -3
};



#endif
