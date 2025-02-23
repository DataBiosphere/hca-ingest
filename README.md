# HCA Ingest

## Running Locally

Follow these steps to set up and run the project locally.

### 1. Clone the Repository

```sh
git clone git@github.com:DataBiosphere/hca-ingest.git
cd hca-ingest
```

### 2. Create and Activate a Python Virtual Environment (!important)

```sh
python -m venv .venv
source .venv/bin/activate
```

### 3. Install Requirements

```sh
pip install -r requirements/base.txt
```

#### If you have issues with **psycopg2-binary** and try and retry install

```shell
brew install postgresql
```

### 4. Set Environment to Local

```sh
export ENVIRONMENT=local
```

### 5. Run Dagster

```sh
dagster dev -w workspace.yaml
```

---

### 6. Visit UI

```shell
127.0.0.1:3000
```

## Running with Docker Compose

Follow these steps to set up and run the project using Docker Compose.

### 1. Clone the Repository

```sh
git clone git@github.com:DataBiosphere/hca-ingest.git
cd hca-ingest
```

### 2. Build and Start the Containers

```sh
docker compose down && docker compose up --build
```

This command will stop any running containers, rebuild the images, and start the services in the background.

### 3. Check Running Containers

```sh
docker ps
```

You should see the **Dagster** and **Postgres** containers running.

### 4. Access Dagster Web UI

Dagster should be running at:

```sh
http://localhost:3000
```

### 5. Stop Containers

To stop and remove running containers, use:

```sh
docker compose down
```

