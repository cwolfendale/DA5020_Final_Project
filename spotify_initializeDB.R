#----------------------------------------------------------------------------------------------------
# Program   : spotify_pull_data.R
#
# Date      : 01 Apr 2018
#
# Programmer: Clare Wolfendale
#
# Purpose   : Program to initialize SQLite DB to store Spotify data
#----------------------------------------------------------------------------------------------------

# imports libraries
library(tidyverse)
library(stringr)
library(RSQLite)

# define root directory path
root <- "C:/Users/cwolfendale/Desktop/Projects/__Courses/Northeastern/DA5020/Final_Project/"

# set working directory to output DB

setwd(str_c(root,"Output"))


# 1. initialize SQLite DB to store Spotify data
#-------------------------------------------------------------------------------------

# open a connection to SQLite and create the SpotifyDB database

db <- dbConnect(SQLite(), dbname="SpotifyDB.sqlite")

# set foreign key constraints to on

dbSendQuery(conn = db, "pragma foreign_keys=on;")


# 2. creates audio features table 
#-------------------------------------------------------------------------------------

# This table will store the audio features for each track_id played
# track_id is the unique identifier for the table

dbSendQuery(conn = db,  "CREATE TABLE audio_features (
            track_id         TEXT PRIMARY KEY,
            track_name       TEXT,
            danceability     REAL,
            energy           REAL,
            key              INTEGER,
            loudness         REAL,
            speechiness      REAL,
            acousticness     REAL,
            instrumentalness REAL,
            liveness         REAL,
            valence          REAL,
            tempo            REAL,
            duration_ms      INTEGER)
            WITHOUT ROWID")


# 3. creates recently played table 
#-------------------------------------------------------------------------------------

# This table will store the track_id and the date-time that each song was played at
# played_at is the unique identifier for the table

dbSendQuery(conn = db,  "CREATE TABLE played_at (
            played_at TEXT PRIMARY KEY,
            track_id  TEXT,
            FOREIGN KEY(track_id) REFERENCES audio_features(track_id))
            WITHOUT ROWID")


# 4. creates artist info table 
#-------------------------------------------------------------------------------------

# This table will store information about the artists for each track_id played
# artist_id is the unique identifier for the table

dbSendQuery(conn = db,  "CREATE TABLE artist_info (
            artist_id   TEXT PRIMARY KEY,
            artist_name TEXT,
            followers   INTEGER,
            popularity  INTEGER)
            WITHOUT ROWID")


# 5. creates track artists table 
#-------------------------------------------------------------------------------------

# This table will store information about the artists for each track_id played
# This table is at the track_id and artist_id level

dbSendQuery(conn = db,  "CREATE TABLE track_artists (
            track_id   TEXT,
            artist_id  TEXT,
            FOREIGN KEY(track_id)  REFERENCES audio_features(track_id),
            FOREIGN KEY(artist_id) REFERENCES artist_info(artist_id))")


# Disconnects from database
dbDisconnect(db)
