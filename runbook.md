# Flux Cloud Benchmark Runbook

Deploys the broker + Postgres + benchmark Job onto EKS / GKE / AKS with the
real object store of each cloud, using `deploy/helm/flux`. Updated as runs
happen; per-cloud results at the bottom.

## Prerequisites (all clouds)

```sh
# Build release binaries (the image build copies them in)
cargo build --release -p flux-broker --bin flux-broker --bin flux-bench

# CLIs: aws (authed via `aws login`), gcloud, az, kubectl, helm, skopeo
```

The container image is built **remotely** (no local Docker needed):
GCP Cloud Build or Azure ACR Tasks build it; `skopeo copy` mirrors it into
ECR for AWS.

Benchmark shape (matches the local 13 GB run): 40 writers × 2,500 requests ×
128 records × 1 KB = 100k requests, 12.8M records. Tune via `--set bench.*`.

---

## AWS (EKS + S3)

```sh
export AWS_REGION=us-east-1 ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export CLUSTER=flux-bench BUCKET=flux-bench-$ACCOUNT

# 1. S3 bucket + ECR repo
aws s3 mb s3://$BUCKET --region $AWS_REGION
aws ecr create-repository --repository-name flux --region $AWS_REGION

# 2. Mirror the image into ECR (built in GAR or ACR below)
aws ecr get-login-password --region $AWS_REGION | skopeo login --username AWS --password-stdin $ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com
skopeo copy docker://us-docker.pkg.dev/zippy-zipline/flux/flux:latest \
  docker://$ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com/flux:latest

# 3. EKS cluster (2 × c6i.8xlarge: 32 vCPU each — one broker node, one bench node)
eksctl create cluster --name $CLUSTER --region $AWS_REGION \
  --nodes 2 --node-type c6i.4xlarge --managed  # 2x16 vCPU fits default quotas

# 4. IRSA: pod identity for S3 access
eksctl utils associate-iam-oidc-provider --cluster $CLUSTER --region $AWS_REGION --approve
eksctl create iamserviceaccount --cluster $CLUSTER --region $AWS_REGION \
  --namespace default --name flux-broker \
  --attach-policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess \
  --approve --role-only --role-name flux-bench-s3
# (chart creates the SA; pass the role annotation via values)

# 5. Deploy
kubectl create configmap flux-migrations --from-file=migrations/001_init.sql
helm install flux deploy/helm/flux \
  --set image.repository=$ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com/flux \
  --set broker.objectStore=s3 --set broker.bucket=$BUCKET \
  --set "serviceAccount.annotations.eks\.amazonaws\.com/role-arn=arn:aws:iam::$ACCOUNT:role/flux-bench-s3"
kubectl rollout status deploy/flux-broker

# 6. Run benchmark
helm upgrade flux deploy/helm/flux --reuse-values --set bench.enabled=true
kubectl logs -f job/$(kubectl get jobs -o name | grep flux-bench | tail -1 | cut -d/ -f2)

# 7. Teardown
helm uninstall flux
eksctl delete cluster --name $CLUSTER --region $AWS_REGION
aws s3 rb s3://$BUCKET --force
aws ecr delete-repository --repository-name flux --region $AWS_REGION --force
```

---

## GCP (GKE + GCS)

```sh
export PROJECT=zippy-zipline REGION=us-central1 CLUSTER=flux-bench
export BUCKET=flux-bench-$PROJECT

# 1. Enable services, GCS bucket, Artifact Registry
gcloud services enable container.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com
gcloud storage buckets create gs://$BUCKET --location=$REGION
gcloud artifacts repositories create flux --repository-format=docker --location=us

# 2. Build image remotely with Cloud Build (uploads context per .gcloudignore,
#    which must include target/release/flux-{broker,bench})
gcloud builds submit --config deploy/docker/cloudbuild.yaml --timeout=20m .

# 3. GKE cluster (2 × c2-standard-30)
gcloud container clusters create $CLUSTER --region $REGION \
  --num-nodes 1 --machine-type c2-standard-30 --workload-pool=$PROJECT.svc.id.goog \
  --node-locations $REGION-a --disk-size 100
gcloud container clusters get-credentials $CLUSTER --region $REGION

# 4. Workload identity: GSA with GCS access bound to the flux-broker KSA
gcloud iam service-accounts create flux-bench
gcloud storage buckets add-iam-policy-binding gs://$BUCKET \
  --member serviceAccount:flux-bench@$PROJECT.iam.gserviceaccount.com --role roles/storage.objectAdmin
gcloud iam service-accounts add-iam-policy-binding flux-bench@$PROJECT.iam.gserviceaccount.com \
  --member "serviceAccount:$PROJECT.svc.id.goog[default/flux-broker]" --role roles/iam.workloadIdentityUser

# 4-alt. No-new-IAM fallback (uses the existing Compute default SA): give the
# node pool storage scopes + legacy metadata, and skip the SA annotation.
gcloud container node-pools create storage-pool --cluster $CLUSTER --region $REGION \
  --num-nodes 2 --machine-type c2-standard-30 --node-locations $REGION-a \
  --scopes gke-default,storage-full --workload-metadata=GCE_METADATA
gcloud container node-pools delete default-pool --cluster $CLUSTER --region $REGION --quiet

# 5. Deploy
kubectl create configmap flux-migrations --from-file=migrations/001_init.sql
helm install flux deploy/helm/flux \
  --set image.repository=us-docker.pkg.dev/$PROJECT/flux/flux \
  --set broker.objectStore=gcs --set broker.bucket=$BUCKET \
  --set "serviceAccount.annotations.iam\.gke\.io/gcp-service-account=flux-bench@$PROJECT.iam.gserviceaccount.com"
kubectl rollout status deploy/flux-broker

# 6. Run benchmark
helm upgrade flux deploy/helm/flux --reuse-values --set bench.enabled=true
kubectl logs -f job/$(kubectl get jobs -o name | grep flux-bench | tail -1 | cut -d/ -f2)

# 7. Teardown
helm uninstall flux
gcloud container clusters delete $CLUSTER --region $REGION --quiet
gcloud storage rm -r gs://$BUCKET
```

---

## Azure (AKS + Blob)

```sh
export RG=flux-bench LOCATION=westus2 CLUSTER=flux-bench
export SA_NAME=fluxbench$RANDOM ACR_NAME=fluxbench$RANDOM

# 1. Resource group, storage account + container, ACR
az group create -n $RG -l $LOCATION
az storage account create -n $SA_NAME -g $RG -l $LOCATION --sku Standard_LRS
az storage container create --account-name $SA_NAME -n flux --auth-mode login
az acr create -n $ACR_NAME -g $RG --sku Basic

# 2. Build image remotely with ACR Tasks
az acr build -r $ACR_NAME -f deploy/docker/Dockerfile -t flux:latest .

# 3. AKS cluster. D32s_v5 needs 64 vCPUs of regional quota; this subscription
# had 38 left in westus2, so 2 × D16s_v5 (fits default quotas).
az aks create -n $CLUSTER -g $RG --node-count 2 --node-vm-size Standard_D16s_v5 \
  --attach-acr $ACR_NAME --generate-ssh-keys
az aks get-credentials -n $CLUSTER -g $RG

# 4. Storage auth: account key via env (simplest for a benchmark)
export AZURE_KEY=$(az storage account keys list -n $SA_NAME -g $RG --query '[0].value' -o tsv)

# 5. Deploy
kubectl create configmap flux-migrations --from-file=migrations/001_init.sql
helm install flux deploy/helm/flux \
  --set image.repository=$ACR_NAME.azurecr.io/flux \
  --set broker.objectStore=azure --set broker.bucket=flux \
  --set "broker.extraEnv[0].name=AZURE_STORAGE_ACCOUNT_NAME,broker.extraEnv[0].value=$SA_NAME" \
  --set "broker.extraEnv[1].name=AZURE_STORAGE_ACCOUNT_KEY,broker.extraEnv[1].value=$AZURE_KEY"
kubectl rollout status deploy/flux-broker

# 6. Run benchmark
helm upgrade flux deploy/helm/flux --reuse-values --set bench.enabled=true
kubectl logs -f job/$(kubectl get jobs -o name | grep flux-bench | tail -1 | cut -d/ -f2)

# 7. Teardown
az group delete -n $RG --yes --no-wait
```

---

## Results

| Cloud | Setup | Produce MiB/s | Fetch MiB/s | Produce p50/p99 ms | Notes |
|---|---|---|---|---|---|
| local | 1 broker proc, local FS, shared 32-core box | 858 | 422 | 386 / 431 | baseline after read-path fix |
| AWS | EKS 2×c6i.4xlarge, S3 (IRSA, bucket-scoped policy) | 775 | 192 | 414 / 520 | 12.8M rec ×1KB; produce 16.1s, fetch 65.0s |
| GCP | — | — | — | — | pending |
| Azure | AKS 2×D16s_v5, Blob (account key), bench+broker on separate nodes | 1,045 | 210 | 299 / 501 | 12.8M rec ×1KB; produce 12.0s, fetch 59.4s |
