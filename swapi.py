#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PROJECT:      Swapi
DESCRIPTION:  Fetches some data from swapi.dev and stores it in postgresql server.
              To not just fetch and store some data blindly the project is limited to the 
              fictional case that the data should be fetched and modeled for 
              someone new to the Star Wars universe who wants to learn the basic 
              facts like amount of films, characters represented in the films, 
              the homeworld of characters.

AUTHOR:       Anna Tykkyläinen
DATE CREATED: 2023-03-10
"""

##############################################################################
###   IMPORT LIBRARIES   #####################################################
import requests
import pandas as pd
import psycopg2

##############################################################################
###   FETCH DATA   ###########################################################
''' 
NOTE: Script below only fetches the first page of results for each resource. 
To fetch all script need to be extended with som logic involving the "next" element of the page.
'''

# Create empty dictionary to store data
data_dict = {}

# Define base url
url_base = "https://swapi.dev/api/"

# Define resources to fetch
resources = ["people",
             "planets",
             "films"]

# Loop through urls and fetch data
for resource in resources:
    url = url_base + resource + "/"
    page = requests.get(url)
    data_dict[resource] = page.json()["results"]

##############################################################################
###   MODEL DATA   ###########################################################
'''
NOTE: Script below includes a lot of repetition which could/should be systemized.
'''

##############################################################################
###   dataframe people
# Create dataframe
people = pd.DataFrame.from_dict(data_dict["people"])

# Select attributes to include and subset dataframe based on these
people_attributes = ["url",
                     "name",
                     "gender",
                     "homeworld"]

people = people[people_attributes]

# Create id based on part of url-string
people["people_id"] = people["url"].str.split("/").str[5]

# Create id:s to be used as foreign keys and rename
people["homeworld"] = people["homeworld"].str.split("/").str[5]
people = people.rename(columns={"homeworld": "planet_id"})


##############################################################################
###   dataframe films
# Create dataframe
films = pd.DataFrame.from_dict(data_dict["films"])

# Select attributes to include and subset dataframe based on these
film_attributes = ["url",
                   "title",
                   "release_date"]

films = films[film_attributes]

# Create id based on part of url-string
films["film_id"] = films["url"].str.split("/").str[5]
 
##############################################################################
###   dataframe people_films
# Create dataframe
people_films = pd.DataFrame.from_dict(data_dict["people"])

# Select attributes to include and subset dataframe based on these
people_film_attributes = ["url",
                          "films"]

people_films = people_films[people_film_attributes]

# Expand rows in dataframe based on list
people_films = people_films.explode('films').reset_index(drop=True) 

# Create id:s to be used as foreign keys and rename
people_films["url"] = people_films["url"].str.split("/").str[5]
people_films["films"] = people_films["films"].str.split("/").str[5]
people_films = people_films.rename(columns={"url": "people_id", "films": "film_id"})

##############################################################################
###   dataframe planets
# Create dataframe
planets = pd.DataFrame.from_dict(data_dict["planets"])

# Select attributes to include and subset dataframe based on these
planet_attributes = ["url",
                     "name"]

planets = planets[planet_attributes]

# Create id based on part of url-string
planets["planet_id"] = planets["url"].str.split("/").str[5]

##############################################################################
###   CONNECT TO DATABASE   ##################################################
# Create database connection
conn = psycopg2.connect(database="database",
                        host="localhost", # TO DO: Vad ska vara här?
                        user="postgres",
                        password="password",
                        port="5432")

# Create cursor
cursor = conn.cursor()

##############################################################################
###   CREATE TABLES   ########################################################
# Create tables
cursor.execute("DROP TABLE IF EXISTS people CASCADE; CREATE TABLE people (people_id integer NOT NULL PRIMARY KEY, url varchar, name varchar, gender varchar, planet_id integer);")

cursor.execute("DROP TABLE IF EXISTS films CASCADE; CREATE TABLE films (film_id integer NOT NULL PRIMARY KEY, url varchar, title varchar, release_date date);")

cursor.execute("DROP TABLE IF EXISTS people_films; CREATE TABLE people_films (people_id integer NOT NULL, film_id integer NOT NULL, FOREIGN KEY(people_id) REFERENCES people(people_id), FOREIGN KEY(film_id) REFERENCES films(film_id));")

cursor.execute("DROP TABLE IF EXISTS planets; CREATE TABLE planets (planet_id integer NOT NULL PRIMARY KEY, url varchar, name varchar);")

# Commit changes
conn.commit()

##############################################################################
###   INSERT DATA   ##########################################################
'''
NOTE: Script below includes a lot of repetition which could/should be systemized.
Also needed is to include try-except for potential errors at insert. 
'''

##############################################################################
###   table people
# Convert dataframe to list of tuples
row_data = [tuple(x) for x in people.to_numpy()]

# Create query to insert to db
add_row_query = """INSERT INTO people (url, name, gender, planet_id, people_id) VALUES (%s, %s, %s, %s, %s)"""

# Loop through list of tuples to insert data to db
for row in row_data:
    cursor.execute(str(add_row_query), row)

# Commit changes
conn.commit()

##############################################################################
###   table films
# Convert dataframe to list of tuples
row_data = [tuple(x) for x in films.to_numpy()]

# Create query to insert to db
add_row_query = """INSERT INTO films (url, title, release_date, film_id) VALUES (%s, %s, %s, %s)"""

# Loop through list of tuples to insert data to db
for row in row_data:
    cursor.execute(str(add_row_query), row)

# Commit changes
conn.commit()

##############################################################################
###   table people_films
# Convert dataframe to list of tuples
row_data = [tuple(x) for x in people_films.to_numpy()]

# Create query to insert to db
add_row_query = """INSERT INTO people_films (people_id, film_id) VALUES (%s, %s)"""

# Loop through list of tuples to insert data to db
for row in row_data:
    cursor.execute(str(add_row_query), row)

# Commit changes
conn.commit()


##############################################################################
###   table planets
# Convert dataframe to list of tuples
row_data = [tuple(x) for x in planets.to_numpy()]

# Create query to insert to db
add_row_query = """INSERT INTO planets (url, name, planet_id) VALUES (%s, %s, %s)"""

# Loop through list of tuples to insert data to db
for row in row_data:
    cursor.execute(str(add_row_query), row)

# Commit changes
conn.commit()

##############################################################################
###   CLOSE DATABASE CONNECTION   ############################################
# Close cursor
cursor.close()    

# Close connection
conn.close()

