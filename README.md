# swapi
## About this project
This project fetches some data from swapi.dev (Star Wars API) and stores it in postgresql server using Docker. 
## Get started
### Setup
Create a project directory.

Change directory to the project directory using command line:

      cd your_directory
      
### Add PostgreSQL server using Docker
Install Docker (if not installed).

Create a Dockerfile in your project directory with the following content:
    
    FROM postgres:alpine

    ENV POSTGRES_USER postgres
    ENV POSTGRES_PASSWORD password
    ENV POSTGRES_DB database

Build the Docker image with the following command using command line:

    docker build -t swapi-postgres .
    
Start the Docker container with the following command using command line:

    docker run -p 5432:5432 --name swapi-postgres-container -it swapi-postgres 

### Run project in Python
Run swapi.py.

