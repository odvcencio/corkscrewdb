# CorkScrewDB / Memory — Deploy Kustomization

Kustomize root for the `m31labs` namespace deployment of CorkScrewDB + Memory.

Apply with:

```
kubectl apply -k deploy/kustomization/
```

## Known-incomplete (Task 13 scaffold)

This kustomization references resources that land in later tasks. Until
Tasks 14–17 land the following files, `kustomize build` / `kubectl apply -k`
dry-run will fail:

- `headless-service.yaml`, `rw-service.yaml`, `ro-service.yaml` (Task 14)
- `statefulset.yaml` (Task 15)
- `memory-deployment.yaml`, `memory-service.yaml` (Task 16)
- `bootstrap-job.yaml` (Task 17)

Only `namespace.yaml`, `secrets.yaml`, and `kustomization.yaml` are present
today. Either wait for later tasks or comment out the missing entries in
`kustomization.yaml` if you need a partial apply.

## Token generation (three Opaque Secrets)

`secrets.yaml` ships stub placeholders (base64 of `REPLACE_WITH_REAL_TOKEN`)
and **must not be applied as-is**. The preferred path is to create each Secret
out-of-band, overriding the stub:

```
# Generate one token per Secret.
CORKSCREW_TOKEN=$(openssl rand -hex 32)
MEMORY_ADMIN_TOKEN=$(openssl rand -hex 32)
MEMORY_TOKEN=$(openssl rand -hex 32)

kubectl -n m31labs create secret generic corkscrewdb-token \
  --from-literal=token="$CORKSCREW_TOKEN"

kubectl -n m31labs create secret generic memory-admin-token \
  --from-literal=token="$MEMORY_ADMIN_TOKEN"

kubectl -n m31labs create secret generic memory-tokens \
  --from-literal=token="$MEMORY_TOKEN"
```

Create the namespace first (`kubectl apply -f namespace.yaml`) or drop
`secrets.yaml` from `kustomization.yaml` and rely entirely on the
out-of-band `kubectl create secret` path.

## Harbor image pull secret

`harbor-m31labs` is **not** in `secrets.yaml`. Create it directly:

```
kubectl -n m31labs create secret docker-registry harbor-m31labs \
  --docker-server=harbor.draco.quest \
  --docker-username=<user> \
  --docker-password=<password> \
  --docker-email=<email>
```

The StatefulSet / Deployment / Job manifests (Tasks 15–17) reference this
secret under `imagePullSecrets`.

## Port-forward recipes

Memory admin API:

```
kubectl -n m31labs port-forward svc/memory 8080:8080
```

CorkScrewDB read-write endpoint:

```
kubectl -n m31labs port-forward svc/corkscrewdb-rw 4040:4040
```

## Rolling upgrade

```
kubectl -n m31labs set image sts/corkscrewdb \
  corkscrewdb=harbor.draco.quest/m31labs/corkscrewdb:<new-tag>
```

Note: CorkScrewDB runs a single writer replica (RW pod). Expect roughly
**10–30 seconds of write unavailability** during a rolling upgrade while the
RW pod terminates and the replacement reaches Ready. Read-only traffic
routed via `svc/corkscrewdb-ro` continues to be served by the remaining
replicas.

Equivalent upgrade for the Memory service:

```
kubectl -n m31labs set image deploy/memory \
  memory=harbor.draco.quest/m31labs/memory:<new-tag>
```
