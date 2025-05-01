# User Guide: E-commerce Event Streaming Pipeline

## Prerequisites

1. **Run the docker compose file  but setup your env variables first

2. **PostgreSQL**:
   - Ensure PostgreSQL is installed and running.
   - Create the database `ecommerce_db` and the table `ecommerce_events`. Use the provided SQL script `postgres_setup.sql` to create the necessary structure.
   - Download and place the PostgreSQL JDBC driver (postgresql-42.7.5.jar) in the project directory.
3. **Python 3.8+**:
   - Install the required dependencies using `pip`:
   ```bash
   