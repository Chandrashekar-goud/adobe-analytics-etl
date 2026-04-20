#!/usr/bin/env bash
# =============================================================
# AWS Infrastructure Setup Script
#
# Run ONCE before the first deployment to create:
#   - 3 S3 buckets (landing / processed / scripts)
#   - IAM role for AWS Glue
#   - Glue job definition
#
# Prerequisites:
#   - AWS CLI installed and configured (aws configure)
#   - Sufficient IAM permissions (or Admin)
#
# Usage:
#   chmod +x infra/setup_aws.sh
#   ENVIRONMENT=dev ./infra/setup_aws.sh
# =============================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration — override via env vars
# ---------------------------------------------------------------------------
ENVIRONMENT="${ENVIRONMENT:-dev}"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
GLUE_JOB_NAME="search-keyword-performance-${ENVIRONMENT}"

LANDING_BUCKET="analytics-landing-${ACCOUNT_ID}-${ENVIRONMENT}"
PROCESSED_BUCKET="analytics-processed-${ACCOUNT_ID}-${ENVIRONMENT}"
SCRIPTS_BUCKET="analytics-scripts-${ACCOUNT_ID}-${ENVIRONMENT}"

GLUE_ROLE_NAME="GlueETLRole-${ENVIRONMENT}"

echo "========================================"
echo "  Environment : ${ENVIRONMENT}"
echo "  Region      : ${REGION}"
echo "  Account ID  : ${ACCOUNT_ID}"
echo "========================================"

# ---------------------------------------------------------------------------
# Helper: create an S3 bucket and apply security baseline
# ---------------------------------------------------------------------------
create_bucket() {
  local bucket_name="$1"
  echo ""
  echo "→ Creating bucket: s3://${bucket_name}"

  # us-east-1 does not accept a LocationConstraint
  if [ "${REGION}" == "us-east-1" ]; then
    aws s3api create-bucket --bucket "${bucket_name}" --region "${REGION}" 2>/dev/null \
      || echo "  (bucket may already exist — skipping)"
  else
    aws s3api create-bucket \
      --bucket "${bucket_name}" \
      --region "${REGION}" \
      --create-bucket-configuration LocationConstraint="${REGION}" 2>/dev/null \
      || echo "  (bucket may already exist — skipping)"
  fi

  # Block ALL public access
  aws s3api put-public-access-block \
    --bucket "${bucket_name}" \
    --public-access-block-configuration \
      "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

  # Enable server-side encryption by default
  aws s3api put-bucket-encryption \
    --bucket "${bucket_name}" \
    --server-side-encryption-configuration \
      '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'

  echo "  ✓ Bucket ready: s3://${bucket_name}"
}

# ---------------------------------------------------------------------------
# Create S3 buckets
# ---------------------------------------------------------------------------
create_bucket "${LANDING_BUCKET}"
create_bucket "${PROCESSED_BUCKET}"
create_bucket "${SCRIPTS_BUCKET}"

# ---------------------------------------------------------------------------
# IAM: Trust policy for Glue
# ---------------------------------------------------------------------------
echo ""
echo "→ Creating IAM role: ${GLUE_ROLE_NAME}"

TRUST_POLICY='{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "glue.amazonaws.com" },
    "Action": "sts:AssumeRole"
  }]
}'

aws iam create-role \
  --role-name "${GLUE_ROLE_NAME}" \
  --assume-role-policy-document "${TRUST_POLICY}" 2>/dev/null \
  || echo "  (role may already exist — skipping)"

# Attach AWS-managed Glue service policy
aws iam attach-role-policy \
  --role-name "${GLUE_ROLE_NAME}" \
  --policy-arn "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"

# Attach inline policy for S3 access (least privilege)
S3_POLICY=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadLanding",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::${LANDING_BUCKET}",
        "arn:aws:s3:::${LANDING_BUCKET}/*"
      ]
    },
    {
      "Sid": "WriteProcessed",
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::${PROCESSED_BUCKET}",
        "arn:aws:s3:::${PROCESSED_BUCKET}/*"
      ]
    },
    {
      "Sid": "ReadScripts",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::${SCRIPTS_BUCKET}",
        "arn:aws:s3:::${SCRIPTS_BUCKET}/*"
      ]
    }
  ]
}
EOF
)

aws iam put-role-policy \
  --role-name "${GLUE_ROLE_NAME}" \
  --policy-name "ETL-S3-Access" \
  --policy-document "${S3_POLICY}"

echo "  ✓ IAM role configured."

# ---------------------------------------------------------------------------
# Create the Glue Job definition
# ---------------------------------------------------------------------------
echo ""
echo "→ Creating Glue job: ${GLUE_JOB_NAME}"

ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${GLUE_ROLE_NAME}"

aws glue create-job \
  --name "${GLUE_JOB_NAME}" \
  --role "${ROLE_ARN}" \
  --command "{
    \"Name\": \"glueetl\",
    \"ScriptLocation\": \"s3://${SCRIPTS_BUCKET}/scripts/main.py\",
    \"PythonVersion\": \"3\"
  }" \
  --default-arguments "{
    \"--extra-py-files\": \"s3://${SCRIPTS_BUCKET}/scripts/src.zip,s3://${SCRIPTS_BUCKET}/scripts/dependencies.zip\",
    \"--output\": \"s3://${PROCESSED_BUCKET}/results/\",
    \"--job-language\": \"python\",
    \"--enable-continuous-cloudwatch-log\": \"true\",
    \"--enable-metrics\": \"true\",
    \"--TempDir\": \"s3://${SCRIPTS_BUCKET}/tmp/\"
  }" \
  --glue-version "3.0" \
  --number-of-workers 2 \
  --worker-type "G.1X" \
  --max-retries 1 \
  --timeout 30 2>/dev/null \
  || echo "  (job may already exist — skipping)"

echo "  ✓ Glue job ready: ${GLUE_JOB_NAME}"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "========================================"
echo "  Setup complete. Add these to GitHub Secrets:"
echo ""
echo "  AWS_ACCESS_KEY_ID     = <your key>"
echo "  AWS_SECRET_ACCESS_KEY = <your secret>"
echo "  AWS_REGION            = ${REGION}"
echo "  S3_LANDING_BUCKET     = ${LANDING_BUCKET}"
echo "  S3_PROCESSED_BUCKET   = ${PROCESSED_BUCKET}"
echo "  S3_SCRIPTS_BUCKET     = ${SCRIPTS_BUCKET}"
echo "  GLUE_JOB_NAME         = ${GLUE_JOB_NAME}"
echo "========================================"