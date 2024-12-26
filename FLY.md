# Deploying PostgreSQL with pgvector on Fly.io

Vector databases have become essential tools for AI and Machine Learning applications, particularly for similarity searches and embeddings storage. While Fly.io offers a standard PostgreSQL image, it doesn't include the popular `pgvector` extension by default. This guide explains how to deploy a PostgreSQL instance with `pgvector` on Fly.io.

## Prerequisites
- Docker installed on your local machine
- Fly.io account and CLI installed
- Docker Hub account

## Steps to Deploy

### 1. Build and Push the Custom Docker Image
Build the custom PostgreSQL image with pgvector support:
```shell
docker build -f Dockerfile-fly -t <DOCKER_HUB_ACCOUNT>/fly-pgvector . --platform "linux/amd64"  
docker push <DOCKER_HUB_ACCOUNT>/fly-pgvector
```
Replace `<DOCKER_HUB_ACCOUNT>` with your Docker Hub username.

### 2. Create PostgreSQL Cluster
Deploy the custom PostgreSQL instance on Fly.io:
```shell
fly pg create --image-ref <DOCKER_HUB_ACCOUNT>/fly-pgvector --app <APP_NAME>
```
Replace `<APP_NAME>` with your desired application name.

### 3. Enable External Access (Optional)
To access the database from outside Fly.io's network:
```shell
fly ips allocate-v4 --app <APP_NAME>
```

This will assign a public IPv4 address to your database instance.

## Next Steps
- Configure your application to connect to the database
- Create tables with vector columns
- Enable the pgvector extension in your database

For more information about pgvector usage, refer to the [official documentation](https://github.com/pgvector/pgvector).