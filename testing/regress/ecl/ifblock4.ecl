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

lOperacaoRaw := RECORD
    STRING cnpj;
    STRING unico;
    STRING prefix;
    STRING numero;
END;

lClienteRaw := RECORD
    STRING tipo;
    STRING identificacao;
END;

lContestacaoRaw := RECORD
    STRING numero;
    STRING tipo;
    STRING hora;
    IFBLOCK(SELF.TIPO = 'O')
        STRING justificativa;
    END;
    DATASET(lClienteRaw) cliente;
    IFBLOCK(SELF.TIPO = 'O')
        DATASET(lOperacaoRaw) operacao;
    END;
END;

mkOp(unsigned i) := TRANSFORM(lOperacaoRaw,
    SELF.cnpj := 'Op' + (STRING)i;
    SELF.unico := (STRING)i + '!';
    SELF.prefix := (STRING)i + '?';
    SELF.numero := (STRING)i + '@';
);

mkCl(unsigned i) := TRANSFORM(lClienteRaw,
    SELF.tipo := 'Cl' + (STRING)i;
    SELF.identificacao := (STRING)i + '!';
);

mkCo(unsigned i) := TRANSFORM(lContestacaoRaw,
    SELF.numero := 'Co' + (STRING)i;
    SELF.tipo := CHOOSE(i % 3, 'O', 'X', 'z');
    SELF.hora := (STRING)i + '?';
    SELF.justificativa := (STRING)i + '*';
    SELF.cliente := DATASET(i % 4, mkCl(COUNTER));
    SELF.operacao := DATASET(i % 5, mkOp(COUNTER));
);

d := DATASET(3*4*5, mkco(COUNTER));
output(count(nofold(d))-3*4*5);
