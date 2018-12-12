#----------------------------------------------------------------------------------------------------
# Program   : spotify_analysis.R
#
# Date      : 21 Apr 2018
#
# Programmer: Clare Wolfendale
#
# Purpose   : Program retrieve and analyze data
#----------------------------------------------------------------------------------------------------

# imports libraries used in program
library(tidyverse)
library(stringr)
library(lubridate)
library(forcats)
library(RSQLite)

# define root directory path
root <- "C:/Users/cwolfendale/Desktop/Projects/__Courses/Northeastern/DA5020/Final_Project/"

# set working directory to output DB

setwd(str_c(root,"Output"))


# 1. initialize connection to SQLite DB and pull data
#--------------------------------------------------------------------------------

# open a connection to thw SpotifyDB database

db <- dbConnect(SQLite(), dbname="SpotifyDB.sqlite")

# set foreign key constraints to on

dbSendQuery(conn = db, "pragma foreign_keys=on;")
dbListTables(db)

# pull complete play history
# convert time stamp to date-time format

played_at_df <- dbGetQuery(conn = db, "SELECT *
                                       FROM played_at") %>% 
  mutate(played_at = ymd_hms(played_at))

# pull subset of audio feature for analysis

audio_features_df <- dbGetQuery(conn = db, "SELECT track_id,
                                                   track_name,
                                                   danceability,
                                                   energy,
                                                   acousticness
                                            FROM audio_features")

# pull subset of artist information for analysis

artist_info_df <- dbGetQuery(conn = db, "SELECT artist_id,
                                                artist_name,
                                                popularity
                                         FROM artist_info")

# pull track and artist crosswalk

track_artist_df <- dbGetQuery(conn = db, "SELECT *
                                          FROM track_artists")

# Disconnects from database
dbDisconnect(db)


# 2. summary statistics from play history
#--------------------------------------------------------------------------------


# count number of tracks listened to

played_at_df %>% 
  count()

# count number of unique tracks listened to

played_at_df %>%
  select(track_id) %>% 
  distinct() %>% 
  count()

# earliest date and lastest dates in database

played_at_df %>% 
  summarise(first_played_at = min(played_at), 
            last_played_at  = max(played_at))

# most listened-to track

played_at_df %>%
  # selects the most listened-to track  
  group_by(track_id) %>% 
  count() %>%
  arrange(desc(n)) %>% 
  head(n = 1) %>% 
  ungroup() %>% 
  # merges on track name and artist name
  left_join(audio_features_df, by = c("track_id"))  %>% 
  left_join(track_artist_df,   by = c("track_id"))  %>%
  left_join(artist_info_df,    by = c("artist_id")) %>%
  select(track_name, artist_name)

# most listened-to artist

played_at_df %>% 
  # merges artsit IDs on to play history
  right_join(track_artist_df, by = c("track_id")) %>% 
  # selects most listened-to artist
  group_by(artist_id) %>% 
  count() %>% 
  arrange(desc(n)) %>% 
  head(n = 1) %>% 
  ungroup() %>% 
  # merges on artist name
  left_join(artist_info_df, by = c("artist_id")) %>% 
  select(artist_name)
  

# 3. avergage number of songs per day of the week
#--------------------------------------------------------------------------------

# calculate the number of songs played on each day

played_at_day_df <- played_at_df %>% 
  mutate(day = as_date(played_at)) %>% 
  group_by(day) %>% 
  summarise(n_day = n())

# calcuate the average number of songs played on each day of the week

played_at_wday_df  <- played_at_day_df %>% 
  mutate(wday = wday(day, label = TRUE, abbr = FALSE)) %>% 
  group_by(wday) %>% 
  summarise(avg_n_wday = mean(n_day)) %>% 
  mutate(avg_n_wday = round(avg_n_wday))

played_at_wday_df

# display in a bar plot

played_at_wday_df %>% 
  ggplot(aes(wday, avg_n_wday, fill = wday)) +
  geom_bar(stat = "identity") +
  xlab("Day of the week") +
  ylab("Average number of tracks per day") +
  theme(legend.position="none")


# 4. avergage number of songs per hour of the day
#--------------------------------------------------------------------------------

# calcuate the number of songs played at each hour of each day

played_at_hour_df <- played_at_df %>% 
  mutate(
    day  = as_date(played_at),
    hour = hour(played_at)
  ) %>% 
  group_by(day, hour) %>% 
  summarise(n_hour = n())

# calcuate the average number of songs played at each hour of the day

played_at_hour_df  <- played_at_hour_df %>% 
  group_by(hour) %>% 
  summarise(avg_n_hour = mean(n_hour)) %>% 
  mutate(avg_n_hour = round(avg_n_hour))

played_at_hour_df

# display in a bar plot

played_at_hour_df %>% 
  ggplot(aes(hour, avg_n_hour)) +
  geom_bar(stat = "identity") +
  xlab("Hour of the day") +
  scale_x_continuous(breaks = seq(1, 24, 1)) +
  ylab("Average number of tracks in hour") +
  theme(legend.position="none")


# 5. avergage popularity of artists listened to by weekday versus weekend
#--------------------------------------------------------------------------------

# calculate average artist popularity by weekday or weekend

played_at_day_type_df <- played_at_df %>% 
  # categorize dates as week day or weekend
  mutate(
    wday     = wday(played_at),
    day_type = ifelse(wday %in% (seq(2,6,1)), 1, 2)
  ) %>%
  # merge on artist IDs
  left_join(track_artist_df, by = c("track_id")) %>% 
  # merge on artist popularity
  left_join(artist_info_df, by = c("artist_id")) %>% 
  group_by(day_type) %>% 
  summarise(avg_pop = mean(popularity))

# add factor levels

played_at_day_type_df <- played_at_day_type_df %>% 
  mutate(
    day_type = factor(day_type),
    day_type = fct_recode(day_type, "Week day" = "1", "Week end" = "2")
  )

# display in a bar plot

played_at_day_type_df %>% 
  ggplot(aes(day_type, avg_pop, fill = day_type)) +
  geom_bar(stat = "identity") +
  xlab("") +
  ylab("Average popularity of artists listened to") +
  theme(legend.position="none")


# 5. avergage energy of songs morning versus night
#--------------------------------------------------------------------------------

# calculate average energy of songs by morning/afternoon or night

played_at_day_time_df <- played_at_df %>% 
  # categorize hours as morning or night
  mutate(
    hour      = hour(played_at),
    time_type = ifelse(hour %in% (seq(3,15,1)), 1, 2)
  ) %>%
  # merge on track audio features
  left_join(audio_features_df, by = c("track_id")) %>% 
  # calculate energy by time type
  group_by(time_type) %>% 
  summarise(avg_energy = mean(energy))

# add factor levels

played_at_day_time_df <- played_at_day_time_df %>% 
  mutate(
    time_type = factor(time_type),
    time_type = fct_recode(time_type, "Morning and afternoon" = "1", "Evening and night" = "2")
  )

# display in a bar plot

played_at_day_time_df %>% 
  ggplot(aes(time_type, avg_energy, fill = time_type)) +
  geom_bar(stat = "identity") +
  xlab("") +
  ylab("Average energy of songs listened to") +
  theme(legend.position="none")

