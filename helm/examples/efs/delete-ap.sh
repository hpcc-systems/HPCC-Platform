#!/bin/bash
WORK_DIR=$(dirname $0)

source ${WORK_DIR}/efs-env

echo "deleting access points for ${EFS_ID}"
# Set the AWS CLI output format to JSON
export AWS_DEFAULT_OUTPUT="json"

# Get the Access Point IDs using `describe-access-points` command and command substitution
access_point_ids=($(aws efs describe-access-points --file-system-id "${EFS_ID}" --query 'AccessPoints[*].AccessPointId' --output text))

# Loop through the access_point_ids array and delete each Access Point
for access_point_id in "${access_point_ids[@]}"; do
  aws efs delete-access-point --access-point-id "$access_point_id"
done