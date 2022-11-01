# Platform Build dockerfile


## ECL code signing within docker containers

 ### github action values

The following values should be added to github action workflows that wish to
utilize the signing behavior.

> sign_modules: ON

Default of sign_modules is `OFF` within the dockerfile. Without this flag turned 
on the build behavior remains exactly the same.

> signing_secret: ${{ secrets.SIGNING_SECRET }}

Default of signing_secret is empty. Populate this variable within your Github
Secrets for the repository with an exported armored secret key.  It is hidden by
Github Actions from view and is hidden from the docker image layers by using 
BuildKit and [secret mounting](https://docs.docker.com.xy2401.com/develop/develop-images/build_enhancements/#new-docker-build-secret-information).
This ensures that the secret key does not leak into the final docker image layers.

> signing_keyid: ${{ secrets.SIGNING_KEYID }}

Default of signing_keyid is `HPCCSystems`.

> signing_passphrase: ${{ secrets.SIGNING_PASSPHRASE }}

Default of signing_passphrase is `none`. This secret should be added to your Github
Secrets for the repository. It is also mounted in and read out using a BuildKit
secret mount so as to not leak into the final docker image layer.