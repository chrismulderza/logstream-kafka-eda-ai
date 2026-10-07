# Predictive AI worker (OpenShift)

Quarkus stream worker: continuous build via OpenShift BuildConfig into
ImageStream `predictive-ai-worker`, Deployment auto-rollout on new `:latest`.

Namespace: `logstream-kafka`. Consumer group: `stream-worker` (ConfigMap).

## Apply

```bash
# 1. Apply manifests (SA, ImageStream, BuildConfig, ConfigMap, Secret, Deployment, Service)
oc apply -k openshift/worker/

# 2. Create or update the Secret (do not commit real keys).
#    If secret.yaml is a placeholder, prefer:
oc create secret generic predictive-ai-worker \
  --from-literal=INFERENCE_API_KEY='your-key' \
  -n logstream-kafka \
  --dry-run=client -o yaml | oc apply -f -
# Or copy secret.example.yaml, fill INFERENCE_API_KEY, then oc apply -f that file.

# 3. Build the image (pick one)
# Binary build from local worker/ (no Git push required):
oc start-build predictive-ai-worker --from-dir=worker --follow
# Or wait for a GitHub/generic webhook after push to main (set webhook secrets first).

# 4. Wait for rollout (image trigger updates the Deployment when :latest changes)
oc rollout status deployment/predictive-ai-worker -n logstream-kafka
```

## Continuous build + deploy

1. **BuildConfig** `predictive-ai-worker` builds `worker/Dockerfile` (Docker strategy)
   from Git `main` / `contextDir: worker`, or from a binary `--from-dir=worker` upload.
2. Build output lands on ImageStreamTag **`predictive-ai-worker:latest`**.
3. Deployment annotation `image.openshift.io/triggers` points container `worker` at
   that ImageStreamTag, so a successful build rolls out a new pod automatically.
4. Replace `CHANGE_ME_*` webhook secrets on the BuildConfig, then point GitHub at
   the webhook URL from `oc describe bc/predictive-ai-worker`.

## Webhook secrets

```bash
oc set build-secret --source bc/predictive-ai-worker -n logstream-kafka  # if using source secrets
# Or edit the BuildConfig triggers and set github/generic secret values, then:
oc describe bc/predictive-ai-worker -n logstream-kafka   # copy Webhook URL
```
