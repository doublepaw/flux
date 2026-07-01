#!/bin/bash
# Initialize S3 bucket for Flux

awslocal s3 mb s3://flux-batches
echo "Created S3 bucket: flux-batches"
