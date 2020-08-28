/* eslint-disable no-undef */
/* eslint-disable @typescript-eslint/no-var-requires */

const fs = require('fs');

global.define = nlsHPCC => {
    fs.writeFileSync('./src/nlsHPCCType.ts', `\
export default interface nlsHPCC {
${Object.keys(nlsHPCC.root).map(id => `    ${id}: string;`).join("\n")}
}
    `);
};

require("../eclwatch/nls/hpcc");
