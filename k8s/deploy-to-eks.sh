#!/bin/bash

# Air Quality Pipeline - EKS Deployment Script
# This script deploys all components to AWS EKS

set -e  # Exit on error

NAMESPACE="airquality"
CITIES="hanoi danang haiphong cantho nhatrang vungtau"

echo "=========================================="
echo "Air Quality Pipeline - EKS Deployment"
echo "=========================================="
echo ""

# Check prerequisites
echo "Checking prerequisites..."
command -v kubectl >/dev/null 2>&1 || { echo "Error: kubectl not found. Please install kubectl."; exit 1; }
command -v aws >/dev/null 2>&1 || { echo "Error: AWS CLI not found. Please install AWS CLI."; exit 1; }

# Check cluster connection
echo "Checking EKS cluster connection..."
if ! kubectl cluster-info &>/dev/null; then
    echo "Error: Cannot connect to Kubernetes cluster."
    echo "Please run: aws eks update-kubeconfig --region <region> --name <cluster-name>"
    exit 1
fi

CLUSTER_NAME=$(kubectl config current-context | cut -d'/' -f2 || echo "unknown")
echo "✅ Connected to cluster: $CLUSTER_NAME"
echo ""

# Step 1: Create namespace
echo "=========================================="
echo "Step 1: Creating namespace"
echo "=========================================="
if kubectl get namespace $NAMESPACE &>/dev/null; then
    echo "⚠️  Namespace $NAMESPACE already exists"
else
    kubectl apply -f namespace.yaml
    echo "✅ Namespace created"
fi
echo ""

# Step 2: Create secrets
echo "=========================================="
echo "Step 2: Creating secrets"
echo "=========================================="
if kubectl get secret airquality-secrets -n $NAMESPACE &>/dev/null; then
    echo "⚠️  Secret airquality-secrets already exists"
    read -p "Do you want to recreate it? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        kubectl delete secret airquality-secrets -n $NAMESPACE
        kubectl apply -f secret-real.yaml
        echo "✅ Secret recreated"
    fi
else
    kubectl apply -f secret-real.yaml
    echo "✅ Secret created"
fi
echo ""

# Step 3: Deploy realtime streaming
echo "=========================================="
echo "Step 3: Deploying realtime streaming (6 cities)"
echo "=========================================="
for city in $CITIES; do
    kubectl apply -f deployment-realtime-${city}.yaml
    echo "✅ Deployed realtime-${city}"
done
echo ""

# Step 4: Deploy archive streaming
echo "=========================================="
echo "Step 4: Deploying archive streaming (6 cities)"
echo "=========================================="
for city in $CITIES; do
    kubectl apply -f deployment-archive-${city}.yaml
    echo "✅ Deployed archive-${city}"
done
echo ""

# Step 5: Deploy analytics cronjobs
echo "=========================================="
echo "Step 5: Deploying analytics cronjobs"
echo "=========================================="
for city in $CITIES; do
    kubectl apply -f cronjob-analytics-hourly-${city}.yaml
    echo "✅ Deployed analytics-hourly-${city}"
done

kubectl apply -f cronjob-analytics-daily.yaml
echo "✅ Deployed analytics-daily"
echo ""

# Show deployment status
echo "=========================================="
echo "Deployment Summary"
echo "=========================================="
echo ""

echo "Namespace:"
kubectl get namespace $NAMESPACE
echo ""

echo "Secrets:"
kubectl get secrets -n $NAMESPACE
echo ""

echo "Deployments (12 total):"
kubectl get deployments -n $NAMESPACE
echo ""

echo "CronJobs (7 total):"
kubectl get cronjobs -n $NAMESPACE
echo ""

echo "Pods:"
kubectl get pods -n $NAMESPACE -o wide
echo ""

echo "=========================================="
echo "✅ Deployment Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Monitor pod status: kubectl get pods -n $NAMESPACE -w"
echo "2. Check logs: kubectl logs <pod-name> -n $NAMESPACE"
echo "3. Check events: kubectl get events -n $NAMESPACE --sort-by='.lastTimestamp'"
echo ""
echo "Expected timeline:"
echo "- Streaming pods: 2-5 minutes to start (Spark JAR download)"
echo "- Analytics hourly: Runs at :05 minutes past each hour"
echo "- Analytics daily: Runs at 00:30 UTC daily"
echo ""
