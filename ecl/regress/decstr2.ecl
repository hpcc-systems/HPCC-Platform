
output('   decimal32_1 = ' + (string)(decimal32_31)1.2 + '\n');

show(real8 value) := sequential(
    output((string) value + ' =\n');
    output('   decimal32_0 = ' + (string)(decimal32_0)value + '\n');
    output('   decimal32_1 = ' + (string)(decimal32_1)value + '\n');
    output('   decimal32_8 = ' + (string)(decimal32_8)value + '\n');
    output('   decimal32_16 = ' + (string)(decimal32_16)value + '\n');
    output('   decimal32_24 = ' + (string)(decimal32_24)value + '\n');
    output('   decimal32_31 = ' + (string)(decimal32_31)value + '\n');
    output('   decimal32_32 = ' + (string)(decimal32_32)value + '\n');
    '----\n');

show(0);
show(1.0);
show(2.1);
show(1230000);
show(0.100000);
show(1234567890123456789.0);
show(0.1234567890123456789);
show(0.000000000000000000001234567890123456789);
show(-1234567.890123456789);
show(1234567890123456789000000000000000.0);
show(1.0e100);
show(1.0e-100);
