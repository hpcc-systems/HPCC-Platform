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
