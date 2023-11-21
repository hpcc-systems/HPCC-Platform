# ESDL Service Deployment

This directory has files and directions showing how you can include an esdl application service as part of an HPCCSystems platform helm install. This is distinguished from an esdl-sandbox application in that it loads its service bundle from a file rather than from dali. 

The primary consideration for this example is the location of the bundle file. When running the helm install, the values you supply must include the path to the bundle file(s) in the `esp.bindings` key. The two possibilites we cover here are just suggestions- you can store your bundle anywhere you like, as long as you supply the correct path in your values file.

For each case below, you can test the running esdl service by navigating to `http://localhost:7770` in your browser. You will see a simple service, WsEchoAddress, that will echo back an address provided on input.

## Bundle in the docker image

In this case you will build a docker image based on a platform image and copy in the bundle file.  Because the helm layer can overwrite what is in the running container, be sure not to use a path that helm mounts a volume to, such as `/etc/config` or `/var/lib/HPCCSystems`. For that reason we use `/home/hpcc/esdl`.

First we build the image, keeping it in the local library, not pushing to any repo:

```
cd helm/examples/esdl/image
docker build -t esdl-echoapp .
```

Now install the helm chart, using the image we just built and our custom values for the esdl service:

```
helm install mycluster hpcc/hpcc -f helm/examples/esdl/values-echo-example-image.yaml --set global.image.version=latest --set global.image.root=library --set global.image.name=esdl-echoapp
```

## Bundle on a persistent volume

In this case the esdl bundle file is located on a persistent volume mounted to the esdl pod. We're extending the `local/hpcc-localfile` example for _Docker desktop (using wsl2 to access windows data)_, so familiarize yourself with that before continuing. The volume mount path is `/var/lib/HPCCSystems/esdl`, with sub-directories named for each ESP instance which holds that ESP's esdl bundles. The ESP instance name in this example is `echo-example`

Some setup is required on your local machine prior to installing the helm charts:

1. Create the host directory `C:\hpccdata`
2. Create subdirectories named `dalistorage`, `debug`, `dropzone`, `esdl\echo-example`, `hpcc-data`, `queries` and `sasha`.
3. Copy `helm/examples/esdl/image/echo_address-bundle.xml` to `C:\hpccdata\esdl\echo-example` so ESP can load it on startup. The ESP does not re-attempt to load bundles after its initial startup. 

Next install the `hpcc-localfile` chart with some custom values:

```
helm install localfile helm/examples/local/hpcc-localfile -f helm/examples/esdl/values-hpcc-localfile.yaml --set common.hostpath=/run/desktop/mnt/host/c/hpccdata
```

Then install the hpcc cluster using custom values to mount our local volumes and run just our `echo-example` esdl application ESP:

```
helm install mycluster hpcc/hpcc --set global.image.version=latest -f helm/examples/esdl/values-echo-example-localfile.yaml
```
