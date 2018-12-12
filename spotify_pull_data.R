#----------------------------------------------------------------------------------------------------
# Program   : spotify_pull_data.R
#
# Date      : 31 Mar 2018
#
# Programmer: Clare Wolfendale
#
# Purpose   : Program to pull data from Spotify, clean, and store it
#----------------------------------------------------------------------------------------------------

# imports libraries used in program
library(tidyverse)
library(stringr)
library(lubridate)
library(httr)
library(jsonlite)
library(RSQLite)

# define root directory path
root <- "C:/Users/cwolfendale/Desktop/Projects/__Courses/Northeastern/DA5020/Final_Project/"

# include spotify utilities

source(str_c(root, "Programs/spotify_utils.R"))

setwd(str_c(root,"Output"))

#-------------------------------------------------------------------------------------
# 1. Pull data from Spotify
#-------------------------------------------------------------------------------------

# a. connect to Spotify API
#-------------------------------------------------------------------------------------

# connect to Spotify using client ID and Client secret for app

spotify_token <- spotify_oauth(
  app_id        = "spotify",
  client_id     = "################################", 
  client_secret = "################################"
  )


# b. pull recently-played tracks from Spotify API
#-------------------------------------------------------------------------------------

# retrieve 50 most recently played tracks from Spotify
# store in data frame

recently_played_df <- get_recently_played(tkn = spotify_token)


# c. pull audio-features for recently-played tracks from Spotify API
#-------------------------------------------------------------------------------------

# generate unique list of track IDs from recently-played tracks

track_ids <- get_track_ids(recently_played_df)

# retrieve audio-feature for tracks
# store in data frame

audio_features_df <- get_audio_features(tkn = spotify_token, track_ids = track_ids) 


# d. pull artist info for recently-played tracks from Spotify API
#-------------------------------------------------------------------------------------

# generate unique list of artist IDs from recently played tracks

artist_ids <- get_artist_ids(recently_played_df)

# retrieve artist info for artists
# store in data frame

artists_info_df <- get_artist_info(tkn = spotify_token, artist_ids = artist_ids) 


# e. create a track artist df from the recently-played tracks df 
#-------------------------------------------------------------------------------------

# return a df with one record per artist per track

track_artist_df <- get_track_artists(recently_played_df)


#-------------------------------------------------------------------------------------
# 2. Clean data for storage in SQLite DB
#-------------------------------------------------------------------------------------

# a. organize table information using normal form rules
#-------------------------------------------------------------------------------------

# move track_name from recently_played into audio_feautures

audio_features_df <- recently_played_df %>% 
  select(track_id, track_name) %>%
  distinct() %>% 
  right_join(audio_features_df, by = c("track_id"))

# drop track_name and artist_id from recently_played
#   artist_id is in the track_artist_df 
#   track_name is in audio_feautures

recently_played_df <- recently_played_df %>% 
  select(played_at, track_id)


# b. clean individual fields in tables
#-------------------------------------------------------------------------------------

# turn played_at into a date-time with lubridate package

recently_played_df <- recently_played_df %>% 
  rename(played_at_str = played_at) %>%   
  mutate(played_at = as.character(ymd_hms(played_at_str, tz = "EST") + hours(1))) %>% 
  select(played_at, track_id)


#-------------------------------------------------------------------------------------
# 3. Store data in SQLite DB
#-------------------------------------------------------------------------------------

# open a connection to thw SpotifyDB database

db <- dbConnect(SQLite(), dbname="SpotifyDB.sqlite")

# set foreign key constraints to on

dbSendQuery(conn = db, "pragma foreign_keys=on;")
dbListTables(db)

# Function to insert data from data frame into table
# Print last 10 records to check insertion

insert_table_data(cn = db, tbl = "audio_features", id = "track_id",  audio_features_df)
insert_table_data(cn = db, tbl = "played_at",      id = "played_at", recently_played_df)
insert_table_data(cn = db, tbl = "artist_info",    id = "artist_id", artists_info_df)
insert_table_data(cn = db, tbl = "track_artists",  id = "track_id",  track_artist_df)

# Disconnects from database
dbDisconnect(db)


