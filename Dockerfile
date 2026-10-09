FROM python:3.12-slim

# Java (11+) is required at build/run time for the PySpark pipeline.
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements-pipeline.txt .
RUN pip install --no-cache-dir -r requirements-pipeline.txt

COPY . .
# Generate raw data, then run the full bronze -> silver -> gold pipeline.
RUN python data/generate.py && python -m src.run

EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
