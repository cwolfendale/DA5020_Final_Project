#----------------------------------------------------------------------------------------------------
# Program   : spotify_utils.R
#
# Date      : 31 Mar 2018
#
# Programmer: Clare Wolfendale
#
# Purpose   : Functions used in final project
#----------------------------------------------------------------------------------------------------

#-------------------------------------------------------------------------------------
# 1. Function to setup connection to Spotify
#-------------------------------------------------------------------------------------

# a. function to generate oauth access token for Spotify
#-------------------------------------------------------------------------------------

spotify_oauth <- function(app_id, client_id, client_secret) {

	# function to set up oauth token for Spotify using functions from httr library
	
  authorize_url <- "https://accounts.spotify.com/authorize"
  access_url    <- "https://accounts.spotify.com/api/token"
  
  spotify_auth <- oauth_endpoint(authorize = authorize_url, access = access_url)  
  spotify_app  <- oauth_app(app_id, client_id, client_secret)
  
  oauth2.0_token(spotify_auth, spotify_app, scope = "user-read-recently-played user-top-read")

}

#-------------------------------------------------------------------------------------
# 2. Functions to pull data from Spotify
#-------------------------------------------------------------------------------------

# a. retrieve recently-played data for a user
#-------------------------------------------------------------------------------------

get_recently_played <- function(tkn) {
  
  # Returns the most recent 50 tracks played by a user. 
  # Note that a track currently playing will not be visible in play history until it has completed. 
  # A track must be played for more than 30 seconds to be included in play history.  
  
  recent_url <- "https://api.spotify.com/v1/me/player/recently-played?limit=50"
  
  # Request to get data from url
  req <- GET(recent_url, config(token = tkn)) %>% 
    content() %>% 
    toJSON()
  
	# Pull items from request  
  items <- fromJSON(req)$items
  
	# Some song have multiple artists
	# Combine artist IDs into single ";" separated string for the track  
  artist_ids <- purrr::map(items$track$artists, `[[`, c("id")) %>% 
    purrr::map(paste, collapse = "; ")

  # Convert to tibble
  tibble(
    played_at    = unlist(items$played_at),
    artist_id    = unlist(artist_ids),
    track_id     = unlist(items$track$id),
    track_name   = unlist(items$track$name)
  )
  
}


# b. retrieve audio-feautures from list of tracks
#-------------------------------------------------------------------------------------

get_track_ids <- function(df) {
  
  # Return a comma-separated list of track IDs from a df of songs
  
  # Retrieve a unique list of track IDs
  df %>% 
    select(track_id) %>% 
    distinct() %>% 
    unlist() %>% 
    paste0(collapse = ",")
  
}

get_audio_features <- function(tkn, track_ids) {
  
  # Get audio features for multiple tracks based on their Spotify IDs. 
  
  audio_features_url <- str_c("https://api.spotify.com/v1/audio-features/?ids=", track_ids)
  
  # Request to get data from url
  req <- GET(audio_features_url, config(token = tkn)) %>% 
    content() %>% 
    toJSON()
  
	# Pull items from request  
  features <- fromJSON(req)$audio_features

  # Convert to tibble
  tibble(
    danceability     = unlist(features$danceability),
    energy           = unlist(features$energy),
    key              = unlist(features$key),
    loudness         = unlist(features$loudness),
    speechiness      = unlist(features$speechiness),
    acousticness     = unlist(features$acousticness),
    instrumentalness = unlist(features$instrumentalness),
    liveness         = unlist(features$liveness),
    valence          = unlist(features$valence),
    tempo            = unlist(features$tempo),
    duration_ms      = unlist(features$duration_ms),
    track_id         = unlist(features$id)
  )
  
}

# c. retrieve artist info from list of tracks
#-------------------------------------------------------------------------------------

get_artist_ids <- function(df) {
  
  # Return a comma-separated list of artist IDs from a df of songs

  # Some songs have multiple artists  
  # Determine the maximum number of artists per track by counting the
  #   number of ";"s plus 1 (";" is the delimter in the artist_id field) 

  n_artists <- df %>% 
    mutate(n_artists = str_count(artist_id, ";") + 1) %>% 
    summarize(max = max(n_artists)) %>% 
    as.integer()
  
  # Separate IDs at the semicolon into separate columns
  # Initialize a vector of column names based on the max number of artists
  artists <- paste0("artist_id_", 1:n_artists)
  
  # Separate the IDs then reshape long and de-dup into a comma-separated list 
  artist_ids <- recently_played_df %>%
    separate(artist_id, into = artists, sep = "; ", remove = FALSE, fill = "right") %>% 
    select(starts_with("artist_id_")) %>% 
    gather(key = "n_artist", value = "artist_id") %>% 
    select(artist_id) %>% 
    filter(!is.na(artist_id)) %>% 
    distinct() %>% 
    unlist() %>% 
    paste0(collapse = ",")
  
}


get_artist_info <- function(tkn, artist_ids) {
  
  # Get Spotify catalog information for several artists based on their Spotify IDs.
   
  artists_url <- str_c("https://api.spotify.com/v1/artists/?ids=", artist_ids)
  
  # request to get data from url
  req <- GET(artists_url, config(token = tkn)) %>% 
    content() %>% 
    toJSON()
  
	# Pull items from request  
  artists <- fromJSON(req)$artists

  # convert to tibble
  tibble(
    artist_id   = unlist(artists$id),
    artist_name = unlist(artists$name),     
    followers   = unlist(artists$followers$total),
    popularity  = unlist(artists$popularity) 
  )
  
}

get_track_artists <- function(df) {
  
  # Return a df containing one record per artist per track
  # This df can be used to link artist info to track info for tracks
  #   with multiple artists
  n_artists <- df %>% 
    mutate(n_artists = str_count(artist_id, ";") + 1) %>% 
    summarize(max = max(n_artists)) %>% 
    as.integer()
  
  # Separate IDs at the semicolon into separate columns
  # Initialize a vector of column names based on the max number of artists
  artists <- paste0("artist_id_", 1:n_artists)
  
  # Separate the IDs then reshape long and de-dup into a comma-separated list 
  recently_played_df %>%
    separate(artist_id, into = artists, sep = "; ", remove = FALSE, fill = "right") %>% 
    select(track_id, starts_with("artist_id_")) %>% 
    gather(-track_id, key = "n_artist", value = "artist_id") %>% 
    select(track_id, artist_id) %>% 
    filter(!is.na(artist_id)) %>% 
    distinct()
  
}

#-------------------------------------------------------------------------------------
# 3. Functions to store data in SQLite DB
#-------------------------------------------------------------------------------------

insert_table_data <- function(cn, tbl, id, df) {
  
  # Insert data into table
  # Insert only records that do not already appear in table
    
  # Pull IDs already in the table
  ids <- dbGetQuery(db, str_c("SELECT DISTINCT ", id, " FROM ", tbl)) %>% 
    as.tibble()
  
  # If the table is empty, add the full df
  # If the table is not empty, add only records not already in table
  if(nrow(ids) > 0) {
    insert_ids <- df %>%
      anti_join(ids, by = c(id))
    if (nrow(insert_ids) > 0) {
      dbWriteTable(conn = cn, name = tbl, value = insert_ids, row.names = FALSE, append = TRUE)
    }
  } else{
    dbWriteTable(conn = cn, name = tbl, value = df, row.names = FALSE, append = TRUE)
  }
  tail(dbReadTable(db, tbl), n = 10) 
}
