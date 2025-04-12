FROM python:3.10-slim

WORKDIR /usr/app

# Install dbt-snowflake adapter instead of dbt-postgres
RUN pip install dbt-snowflake==1.5.3 

# Copy project files
COPY . /usr/app/

# Keep container running
CMD ["tail", "-f", "/dev/null"]
