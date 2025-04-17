FROM python:3.10-slim

WORKDIR /usr/app

# Install dbt-snowflake adapter
RUN pip install dbt-snowflake==1.5.3 

# Copy project files first (packages.yml and dbt_project.yml)
COPY dbt_project.yml packages.yml* /usr/app/

# Install dbt packages during build
RUN if [ -f "packages.yml" ]; then dbt deps --profiles-dir .; else echo "No packages.yml found"; fi

# Copy the rest of the project files
COPY . /usr/app/

# Ensure latest packages are installed on container start (in case of volume mounts)
ENTRYPOINT ["sh", "-c", "dbt deps --profiles-dir . && tail -f /dev/null"]
