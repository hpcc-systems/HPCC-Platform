# S3 Storage Plane — Deployment Guide

## Overview

Deploy HPCC with an S3 storage plane on EKS, using IRSA for authentication.

## Prerequisites

- EKS cluster with HPCC deployed via Helm
- AWS CLI, kubectl, helm, eksctl

## Setup

### 1. Create S3 Bucket and IRSA

```sh
aws s3 mb s3://<BUCKET> --region us-east-1

aws iam create-policy --policy-name hpcc-s3-access --policy-document '{
  "Version":"2012-10-17",
  "Statement":[{"Effect":"Allow",
    "Action":["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:ListBucket",
              "s3:GetBucketLocation","s3:AbortMultipartUpload",
              "s3:ListMultipartUploadParts","s3:HeadObject"],
    "Resource":["arn:aws:s3:::<BUCKET>","arn:aws:s3:::<BUCKET>/*"]}]}'

eksctl create iamserviceaccount \
  --name hpcc-default --namespace default \
  --cluster <CLUSTER> --region us-east-1 \
  --attach-policy-arn arn:aws:iam::<ACCOUNT>:policy/hpcc-s3-access \
  --approve --override-existing-serviceaccounts
```

### 2. Configure Storage Plane

Edit `values-s3-eks.yaml` with your ECR repo, bucket name, and cluster topology. The key section is the S3 storage plane:

```yaml
storage:
  planes:
  - name: s3data
    prefix: "s3:s3data"
    category: data
    storageapi:
      type: s3
      region: us-east-1
      buckets:
      - name: <BUCKET>
```

See `values-s3.yaml` for a minimal generic example.

### 3. Deploy

```sh
helm upgrade myhpcc hpcc/hpcc -f values-s3-eks.yaml
```

### 4. Verify

```ecl
ds := DATASET([{'hello'}], {STRING10 val});
OUTPUT(ds,, '~test::s3_verify', OVERWRITE, PLANE('s3data'));
OUTPUT(DATASET('~test::s3_verify', {STRING10 val}, FLAT));
```

Check data in S3:
```sh
aws s3 ls s3://<BUCKET>/ --recursive --human-readable
```

## Overlay Build (for stock platform-core images)

If building the S3 hook as an overlay on a stock `platform-core` image:

```sh
docker build --platform linux/amd64 \
  -t <ACCOUNT>.dkr.ecr.us-east-1.amazonaws.com/hpcc-s3-dev:<TAG> \
  -f dockerfiles/s3-hook-overlay.dockerfile .

docker push <ACCOUNT>.dkr.ecr.us-east-1.amazonaws.com/hpcc-s3-dev:<TAG>
```

Then set `global.image` in your Helm values to point to the overlay image.

## Known Limitations

- Helm chart schema may need patching to accept `s3` as a `storageapi.type` until natively supported
- hthor requires separate IRSA setup on its service account (`hpcc-agent`)
- Recursive directory listing (`sub=true`) not implemented
- `jplane_compat.hpp` uses GCC-mangled symbol names for cross-version builds

## Cleanup

```sh
aws s3 rm s3://<BUCKET>/ --recursive
aws s3 rb s3://<BUCKET>
eksctl delete iamserviceaccount --name hpcc-default --namespace default \
  --cluster <CLUSTER> --region us-east-1
```
