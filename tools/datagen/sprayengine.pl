################################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
################################################################################

use strict;

my $group = "thor_data100";
my $cluster = "thor_hank";
my $inputQ = "thor_data400::sprayed_data";
#my $inputQ = "thor_hank::sprayed_data";

my $roxieip = "10.150.68.201:9876";

my $lzip = "10.150.12.241";
my $lzname = "testbed.br.seisint.com";
my $lzuser = "hozed";
my $lzpasswd = "l\@wn\&g\@rd3n";

my $panic = 0;

while (1)
{
    my $files = `/putty/plink.exe -pw $lzpasswd -l $lzuser $lzname (cd /dod/datagen; ls *.d00)`;
    $files =~ s/\s+$//;
    $files =~ s/\s+/,/g;
        
    if (length($files) > 0)
    {
        print "Starting dfu import\n";

        open(FD, ">import_test.def");
        print FD "filename=$files\n";
        print FD "directory=/dod/datagen\n";
        print FD "format=csv\n";
        print FD "$lzip\n";
        close(FD);

        my $ofname = time();
        my $lfname = $cluster . "::victor::" . $ofname;
        my $cmdline;
        my $i;
        my $rc;

        open(FD, ">import_test.dfu");
        print FD "filename=$ofname._\$P\$_of_\$N\$\n";
        print FD "directory=c:\\thordata\\$cluster\\victor\n";
        print FD "group=$group\n";
        close(FD);

        for ($i=0; $i<3; $i++)
        {
            $cmdline = "dfu IMPORT import_test.def $lfname import_test.dfu /NI";
            $rc = 0xffff & system($cmdline);

            if ($rc == 0)
            {
                last;
            }

            print "Retrying spray\n";
        }

        if ($rc == 0)
        {
            $files =~ s/,/ /g;
            $cmdline = "/putty/plink.exe -pw $lzpasswd -l $lzuser $lzname (cd /dod/datagen; mv $files done)";
            system($cmdline);

            $cmdline = "dfu ADDSUPER $inputQ $lfname /NI";
            system($cmdline);
        }

        open(FD, ">tmp.ecl");
        if ($panic == 0)
        {
            print FD "Dod_Demo.mac_daily_fromdata(false);\n";
        }
        else
        {
            print FD "Dod_Demo.mac_daily_fromdata(true);\n";
        }
        close(FD);

        # call eclplus
        my $timein = time();
        system("eclplus \@tmp.ecl");
        # notify roxie
        system("/roxieconfig/roxieconfig Dod_demo.alpha_service update_superkeys=1 deploy=1 roxie=$roxieip delete_query=1");
        my $timeout = time();

        my $timetook = $timeout - $timein;
        print "mac_daily_fromdata took $timetook secs\n";

        if (($timeout - $timein) < 240)
        {
            print "less than 4 minutes ... setting panic to true\n";
            $panic = 1;
        }
        elsif (($timeout - $timein) > 480)
        {
            if ($panic == 1)
            {
                print "mac_daily_fromdata(true) took more than 8 minutes ... setting panic to false\n";
                $panic = 0; 
            }
            else
            {
                print "mac_daily_fromdata(false) took more than 8 minutes ... time to MAC_integrate_day(false)\n";
                # fork MAC_Integrate_Day
                if (fork() == 0)
                {
                    open(FD, ">tmp2.ecl");
                    print FD "Dod_Demo.mac_integrate_day(false);\n";
                    close(FD);

                    # call eclplus
                    system("eclplus cluster=400way \@tmp2.ecl");

                    # notify roxie
                    system("/roxieconfig/roxieconfig Dod_demo.alpha_service update_superkeys=1 deploy=1 roxie=$roxieip delete_query=1");

                    exit;
                }
            }
        }
    }

    my $stime = (15*60+2) - (time() % (15*60));
    print "Calling sleep $stime\n";
    sleep $stime;
}