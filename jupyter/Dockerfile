# Use a stable and feature-rich Jupyter image with Spark
FROM jupyter/pyspark-notebook:latest

# Switch to root to install system packages
USER root

# Install spaCy and English language model
RUN pip install --no-cache-dir spacy==2.3.9 && \
    python -m spacy download en_core_web_sm

# Revert to default notebook user
USER ${NB_UID}
