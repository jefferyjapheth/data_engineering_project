FROM quay.io/astronomer/astro-runtime:3.0.2

USER root

# Handle airflow user/group creation with proper error handling
RUN if getent group airflow > /dev/null; then \
        echo "Group airflow already exists"; \
    else \
        groupadd -g 50000 airflow || groupadd airflow; \
    fi && \
    if id airflow &> /dev/null; then \
        echo "User airflow already exists"; \
    else \
        useradd -m -u 50000 -g airflow airflow || useradd -m -g airflow airflow; \
    fi

# Create and set permissions for airflow directories
RUN mkdir -p /opt/airflow && \
    chown -R airflow:airflow /opt/airflow && \
    chmod -R 775 /opt/airflow

# Upgrade pip first
RUN pip install --upgrade pip

# Copy requirements.txt and install python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --retries 3 --timeout 120 -r /requirements.txt

# Switch to airflow user
USER airflow

# Set working directory to Airflow home directory
WORKDIR /opt/airflow
