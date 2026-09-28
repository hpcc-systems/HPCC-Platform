/*##############################################################################

    Copyright (C) 2026 HPCC Systems®.

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

#pragma once

#include "eventoperation.h"

class event_decl CIndexLoadSequenceOp : public CEventConsumingOp
{
public:
    CIndexLoadSequenceOp();
    virtual bool ready() const override;
    virtual bool doOp() override;

    void setReadSize(__uint64 value) { readSize = value; }

private:
    __uint64 readSize{0};
};
