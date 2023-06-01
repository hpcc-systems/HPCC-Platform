import format;

pwd := 'p@ssw0rd!';
income := 10.12512;

output('pwd: ' + format.maskPassword(pwd) + ' - ' + format.formatMoney(income));

output('pwd: ' + format.maskPassword('x') + ' - ' + format.formatMoney(1.23));
