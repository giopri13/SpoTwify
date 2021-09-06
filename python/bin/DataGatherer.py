import tweepy
import spotipy
import spotipy.oauth2 as ouath2
import json
from tweepy import OAuthHandler
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOAuth
from kafka import KafkaProducer
from time import sleep

kafkaProducer = KafkaProducer(bootstrap_servers = ['10.0.100.23:9092'],
                                value_serializer = lambda data:
                                json.dumps(data).encode("utf-8"))

# Initialize Spotify instance
def initializeSpotify():
    spotify_client_id = "923ce40baff4430f8a85a388559655b2"
    spotify_client_secret = "66bdd0c22a9a4a648143d56764787982"
    spotify_scope = ""
    spotify_redirect_uri = "https://spotify.com"
    spotify = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id = spotify_client_id,
                                                        client_secret = spotify_client_secret,
                                                        redirect_uri = spotify_redirect_uri,
                                                        scope = spotify_scope,
                                                        open_browser = False))
    return spotify

# Extracts track info and returns a dictionary with keys {Artist, Song, URL}
def extract_track_info(spotifyTrack):
    track_artist = spotifyTrack["artists"][0]["name"]
    track_title = spotifyTrack["name"]
    track_url = spotifyTrack["external_urls"]["spotify"]
    track_info = {
        'Artist' : track_artist,
        'Title' : track_title,
        'URL' : track_url
    }
    return track_info

# Retrieves a list containing dictionaries with keys {Artist, Song, URL} for each song in the Top 50 Global Playlist from Spotify
def get_spotify_top50_playlist(spotifyInstance):
    top50_playlist = spotifyInstance.playlist(playlist_id = "37i9dQZEVXbMDoHDwVN2tF")
    playList = []
    for item in top50_playlist["tracks"]["items"]:
        playList.append(extract_track_info(item["track"]))
    return playList

# Initialize the Authentication Handler for Twitter
def initializeAuthenticationHandler():
    twitter_consumer_key = "ynM65RLauKubxd73PJl9Z7kwT"
    twitter_consumer_secret = "FEJ7Nj7CASM3tLBXDuMWduLtZ7qiPsnukDofWVxMWkpJYiJiL9"
    twitter_access_token = "1384458314441007105-pqxRc61p0BSPUJ0tZTmevDwcK9n77D"
    twitter_access_token_secret = "KayDJPAxoCpvXwZ4gqg6Haf7RkkAYXkSs0fuPJN08RQ8g"
    twitter_authentication_handler = OAuthHandler(consumer_key = twitter_consumer_key,
                                                consumer_secret = twitter_consumer_secret)
    twitter_authentication_handler.set_access_token(key = twitter_access_token,
                                                    secret = twitter_access_token_secret)
    return twitter_authentication_handler

# Initialize Twitter instance
def initializeTwitter():
    twitter = tweepy.API(initializeAuthenticationHandler(),
                        wait_on_rate_limit = True)
    return twitter

def main():
    spotifyAPI = initializeSpotify()
    twitterAPI = initializeTwitter()
    print("Retrieving playlist...")
    top50_playlist = get_spotify_top50_playlist(spotifyAPI)
    print("{} Playlist retrieved".format(top50_playlist))
    kafkaProducer.send("spotify", value = top50_playlist)
    sleep(3)
    url_filter = []
    for song in top50_playlist:
        url_filter.append(song.get("URL"))
    
    print("Searching for tweets based on Spotify URLs...")
    json_urlField = "URL"
    json_tweetField = "tweet"
    for url in url_filter:
        tweetsQuery = tweepy.Cursor(twitterAPI.search, q = url, lang = "en").items()
        for tweet in tweetsQuery:
            json_output = '{"'+json_urlField+'":"'+url+'","'+json_tweetField+'":"'+tweet.text+'"}'
            dict_output = {'URL': url, 'Tweet': tweet.text}
            kafkaProducer.send("tweets", value = json_output)
    print("Twitter search finished")
    
    """
    song_filter = []
    for song in top50_playlist:
        artist = song.get("Artist")
        title = song.get("Title")
        song_filter.append(artist + " - " + title)
    print("Searching for tweets about songs...")
    for song in song_filter:
        tweetsQuery = tweepy.Cursor(twitterAPI.search, q = song, lang = "en").items()
        for tweet in tweetsQuery:
            kafkaProducer.send("tweets", value = tweet.text)
    print("Twitter search finished")
    """

main()