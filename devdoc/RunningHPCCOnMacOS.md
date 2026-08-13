# Configure a Local HPCC Platform Runtime on macOS

HPCC Platform's default bare-metal configuration assumes Linux network and
process behavior. After installing HPCC Platform on macOS, make the following
changes before launching the cluster for the first time.

The installed configuration files are normally in `/etc/HPCCSystems`. For an
installation staged with `DESTDIR`, they are in
`<DESTDIR>/etc/HPCCSystems`. Use the location that matches the installation.

## Bind services to the loopback interface

Edit the installed `environment.conf` and set `interface` to the macOS loopback
interface:

```ini
interface=lo0
```

The Linux loopback interface name `lo` is not valid on macOS.

## Disable Roxie background nice adjustment

macOS does not permit the Roxie background workers to use the default nice
adjustment in the same way as Linux. Edit the installed `environment.xml` and
add `adjustBGThreadNiceValue="0"` to the main `RoxieCluster` element:

```xml
  <RoxieCluster allFilesDynamic="true"
                adjustBGThreadNiceValue="0"
                allowedPipePrograms="*">
    ...
  </RoxieCluster>
```

Add the attribute only to the `RoxieCluster` definition under `Software`, not
to the later topology references such as `<RoxieCluster process="myroxie"/>`.