/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#option ('globalFold', false);

export sys :=
    SERVICE
varstring strdup(const varstring src) : c,sys,entrypoint='strdup';
    END;



export display :=
    SERVICE
        echo(const string src) : eclrtl,library='eclrtl',entrypoint='rtlEcho';
    END;


//(string10)(string20)((string)(1+0)+(string)(1.1+0)) = (string10)('123456789'+'123456');
display.echo(sys.strdup('Gavin') +  ' ' + (sys.strdup('Jingo'))[2..2+2] + ' ' + (varstring)((decimal10_2)10.2 + (decimal10_2)0) + ' = Gavin ing 10.2');
