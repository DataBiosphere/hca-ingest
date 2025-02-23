FROM python:3.9-slim

WORKDIR /code

COPY requirements/base.txt .
RUN pip install --no-cache-dir -r base.txt

COPY . .

CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000", "-w", "/code/workspace.yaml"]
