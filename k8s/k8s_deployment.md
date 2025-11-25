# Kubernetes Deployment Walkthrough

This guide details how to deploy the airquality application and its infrastructure to a Kubernetes cluster.

## Prerequisites
- Docker installed (Docker Desktop or Docker Engine)
- Kubernetes cluster (Minikube, Docker Desktop, etc.)
- `kubectl` configured

### Installing Minikube on WSL (Windows Subsystem for Linux)

If you are using WSL, follow these steps to install Minikube:

1.  **Download the binary:**
    ```bash
    curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
    ```

2.  **Install Minikube:**
    ```bash
    sudo install minikube-linux-amd64 /usr/local/bin/minikube
    ```

3.  **Start Minikube:**
    ```bash
    minikube start
    ```
    *Note: If you encounter issues with drivers, you might need to specify the driver, e.g., `minikube start --driver=docker`.*

## 1. Build the Application Image
First, you need to build the Docker image for the Python application.

```bash
docker build -t airquality-app:latest .
```

> [!NOTE]
> If you are using Minikube, you might need to point your shell to Minikube's Docker daemon so that the cluster can see the image you built:
> ```bash
> eval $(minikube docker-env)
> ```

## 2. Apply Kubernetes Manifests
Apply the manifests in the `k8s` directory.

```bash
# Create Namespace
kubectl apply -f k8s/namespace.yaml

# Apply ConfigMap
kubectl apply -f k8s/configmap.yaml

# Apply Infrastructure Services
kubectl apply -f k8s/zookeeper.yaml
kubectl apply -f k8s/kafka.yaml
kubectl apply -f k8s/schema-registry.yaml
kubectl apply -f k8s/timescaledb.yaml
kubectl apply -f k8s/minio.yaml
kubectl apply -f k8s/grafana.yaml
kubectl apply -f k8s/redpanda-console.yaml

# Apply Application Deployment
kubectl apply -f k8s/app-deployment.yaml
```

## 3. Verify Deployment
Check the status of the pods.

```bash
kubectl get pods -n airquality
```

Wait until all pods are in `Running` state.

## 4. Access Services
You can access the services using port forwarding.

### Redpanda Console (Kafka UI)
```bash
kubectl port-forward svc/redpanda-console 8080:8080 -n airquality
```
Open [http://localhost:8080](http://localhost:8080)

### Grafana
```bash
kubectl port-forward svc/grafana 3000:3000 -n airquality
```
Open [http://localhost:3000](http://localhost:3000) (User: `admin`, Password: `admin`)

### MinIO Console
```bash
kubectl port-forward svc/minio 9001:9001 -n airquality
```
Open [http://localhost:9001](http://localhost:9001) (User: `minioadmin`, Password: `minioadmin`)

## 5. Check Application Logs
```bash
kubectl logs -l app=airquality-app -n airquality -f
```