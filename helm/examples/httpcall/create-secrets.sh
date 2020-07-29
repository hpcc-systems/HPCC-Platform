kubectl create secret generic http-connect-urlsecret --from-file=url=url-combined

kubectl create secret generic http-connect-basicsecret --from-file=url=url-basic --from-file=username --from-file=password

kubectl create secret generic http-connect-certsecret --from-file=url=url-cert  --from-file=tls.key --from-file=tls.crt
