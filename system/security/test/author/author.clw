;/*##############################################################################
;
;    Copyright (C) 2011 HPCC Systems.
;
;    This program is free software: you can redistribute it and/or modify
;    it under the terms of the GNU Affero General Public License as
;    published by the Free Software Foundation, either version 3 of the
;    License, or (at your option) any later version.
;
;    This program is distributed in the hope that it will be useful,
;    but WITHOUT ANY WARRANTY; without even the implied warranty of
;    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;    GNU Affero General Public License for more details.
;
;    You should have received a copy of the GNU Affero General Public License
;    along with this program.  If not, see <http://www.gnu.org/licenses/>.
;############################################################################## */

; CLW file contains information for the MFC ClassWizard

[General Info]
Version=1
LastClass=CAuthorDlg
LastTemplate=CDialog
NewFileInclude1=#include "stdafx.h"
NewFileInclude2=#include "author.h"

ClassCount=2
Class1=CAuthorApp
Class2=CAuthorDlg

ResourceCount=3
Resource2=IDR_MAINFRAME
Resource3=IDD_AUTHOR_DIALOG

[CLS:CAuthorApp]
Type=0
HeaderFile=author.h
ImplementationFile=author.cpp
Filter=N

[CLS:CAuthorDlg]
Type=0
HeaderFile=authorDlg.h
ImplementationFile=authorDlg.cpp
Filter=D
LastObject=CAuthorDlg
BaseClass=CDialog
VirtualFilter=dWC



[DLG:IDD_AUTHOR_DIALOG]
Type=1
Class=CAuthorDlg
ControlCount=22
Control1=IDC_STATIC,static,1342308352
Control2=IDC_USER,edit,1350631552
Control3=IDC_STATIC,static,1342308352
Control4=IDC_PASSWORD,edit,1350631584
Control5=IDC_FEATURE_A,button,1342242819
Control6=IDC_FEATURE_B,button,1342242819
Control7=IDC_FEATURE_C,button,1342242819
Control8=IDC_FEATURE_D,button,1342242819
Control9=IDC_FEATURE_E,button,1342242819
Control10=IDC_FEATURE_F,button,1342242819
Control11=IDC_FEATURE_G,button,1342242819
Control12=IDC_FEATURE_H,button,1342242819
Control13=IDC_FEATURE_I,button,1342242819
Control14=IDC_BTN_AUTHORIZE,button,1342242816
Control15=IDC_STATIC,button,1342177287
Control16=IDC_STATIC,static,1342308352
Control17=IDC_SERVICE_NAME,edit,1350631552
Control18=IDC_BTN_CREATE_SEC_MGR,button,1342242816
Control19=IDC_STATIC,button,1342177287
Control20=IDC_PERMISSIONS,edit,1350635652
Control21=IDC_BTN_CREATE_USER,button,1342242816
Control22=IDC_BTN_BUILD_FEATURE_SET,button,1342242816

