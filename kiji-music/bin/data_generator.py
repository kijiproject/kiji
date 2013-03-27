#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import datetime
import os
import random
import sys
import time


def BuildFlagParser():
  """Builds a flag parser for the KijiMusic data generator tool.

  Returns:
    Command-line flag parser.
  """
  parser = argparse.ArgumentParser(
    description='KijiMusic data generator.'
  )
  parser.add_argument(
    '--output-dir',
    dest='output_dir',
    type=str,
    default=os.getcwd(),
    help='Output directory where to write generated data files',
  )

  parser.add_argument(
    '--nusers',
    type=int,
    default=50,
    help='Number of users',
  )
  parser.add_argument(
    '--nsongs',
    type=int,
    default=50,
    help='Number of songs',
  )
  parser.add_argument(
    '--nusers-per-cluster',
    type=int,
    default=10,
    help='Number of users per cluster',
  )
  parser.add_argument(
    '--ngenres',
    type=int,
    default=10,
    help='Number of genres',
  )
  return parser


# Parsed command-line flags:
global FLAGS
FLAGS = None


class KijiMusicDataGenerator(object):
  """Data generator for the KijiMusic tutorial."""

  def __init__(self):
    """Initializes a new data generator."""
    pass

  def ConfigureFromFlags(self, flags):
    """Configures this generator according to command-line flags."""
    self._output_dir = flags.output_dir
    if not os.path.exists(self._output_dir):
      os.makedirs(self._output_dir)
    assert os.path.isdir(self._output_dir), (
      'Invalid output directory: %s' % self._output_dir)

    self._nusers = flags.nusers
    self._nsongs = flags.nsongs
    self._nusers_per_cluster = flags.nusers_per_cluster
    self._ngenres = flags.ngenres

  def Generate(self):
    """Generates data for the KijiMusic tutorial."""
    # TODO: Break this in several smaller methods.

    num_users = self._nusers
    num_songs = self._nsongs
    num_users_per_cluster = self._nusers_per_cluster
    num_genres = self._ngenres

    num_clusters = num_users / num_users_per_cluster
    num_songs_per_cluster = num_songs / num_clusters

    avg_plays_noncluster = 3          # Average plays for a song not in the cluster
    avg_plays_cluster = 10            # Average plays for a song in the cluster
    avg_plays_popular = 25            # Average plays for a popular song in the cluster
    max_popular_songs_in_cluster = 3  # Maximum number of popular songs in a cluster

    prob_of_non_cluster_play = .2     # Probability of a non-cluster song being played

    start_time = datetime.datetime(2012, 1, 4, 17)

    # Before data generation, we'll determine which songs will be the
    # popular songs in each cluster. The number of popular songs in a
    # cluster will be chosen randomly between [1, # Max popular songs]
    popular_songs = []
    popular_songs_in_cluster = 0
    for song_id in range (0, num_songs):
      cluster_index = song_id % num_songs_per_cluster
      if cluster_index == 0:
        popular_songs_in_cluster = round(random.uniform(1, max_popular_songs_in_cluster))
      if cluster_index < popular_songs_in_cluster:
        popular_songs.append(True)
      else:
        popular_songs.append(False)
    del song_id

    # Generate and output song metadata
    song_metadata_path = os.path.join(self._output_dir, "song-metadata.json")
    assert not os.path.exists(song_metadata_path), (
      'Song metadata file already exists: %s' % song_metadata_path)
    print('Writing song metadata to %s...' % song_metadata_path)
    with open(song_metadata_path, 'w') as f:
      artist_id = 0
      album_id = 0
      genre = 'genre' + str(round(random.uniform(0, num_genres)))
      song_metadata = []
      for song_id in range(0, num_songs):
        song_key = 'song-%d' % song_id

        if song_id % 10 == 0:
          if album_id % 2 == 0:
            artist_id = artist_id + 1
            # Albums from a particular artist are likely in one genre
            genre = 'genre' + str(round(random.uniform(0, num_genres)))
          album_id = album_id + 1
        song_name = 'song name-%d' % song_id
        artist_name = 'artist-%d' % artist_id
        album_name = 'album-%d' % (album_id % 2)
        tempo = 60 + 10 * round(random.gauss(6, 4))
        duration = 120 + 60 * round(random.gauss(1, 1))
        song_metadata.append({ 'duration': duration })

        f.write('{ "song_id" : "%s", "song_name" : "%s", "artist_name" : "%s", '
                '"album_name" : "%s", "genre" : "%s", "tempo" : "%d", "duration" : "%d" }\n'
              % (song_key, song_name, artist_name, album_name, genre, tempo, duration))
    del song_id


    # Generate the set of track plays for each user
    user_features = []
    for user_id in range(0, num_users):
      user_cluster = user_id / num_users_per_cluster
      user_features.append([])

      # For each user, each song will make be a single feature
      for song_id in range(0, num_songs):
        song_cluster = song_id / num_songs_per_cluster

        # For now, clusters are evenly divided, and each user will
        # belong to a particular cluster. How many plays a song
        # will get depends on whether or not the song is in the
        # user's cluster
        if song_cluster != user_cluster:
          should_play = random.random() <= prob_of_non_cluster_play
          if should_play:
            num_plays = round(random.uniform(1, avg_plays_noncluster))
            user_features[user_id].append(int(num_plays))
          else:
            user_features[user_id].append(0)
        elif popular_songs[song_id]:
          # Song is in the cluster for this user and is popular
          num_plays = round(random.gauss(avg_plays_popular, 5))
          user_features[user_id].append(int(num_plays))
        else:
          num_plays = round(random.gauss(avg_plays_cluster, 3))
          user_features[user_id].append(int(num_plays))
    del user_id, song_id

    # Generate a random order of plays for each user
    track_plays = []
    for user_id in range(0, num_users):
      track_plays.append([])
      for song_id in range(0, num_songs):
        for k in range(0, user_features[user_id][song_id]):
          track_plays[user_id].append(song_id)
      random.shuffle(track_plays[user_id])
    del user_id, song_id

    # Print the data distribution
    song_dist_path = os.path.join(self._output_dir, "song-dist.json")
    assert not os.path.exists(song_dist_path), (
      'Song distribution file already exists: %s' % song_dist_path)
    print('Writing song distribution data to %s...' % song_dist_path)
    with open(song_dist_path, 'w') as f:
      for i in range(0, num_users):
        for j in range(0, len(user_features[i])):
          if j > 0:
            f.write(', ')
          f.write('%s' % (int(user_features[i][j]),))
        f.write('\n')

    # Print the track plays
    song_plays_path = os.path.join(self._output_dir, "song-plays.json")
    assert not os.path.exists(song_plays_path), (
      'Song plays file already exists: %s' % song_plays_path)
    print('Writing song plays data to %s...' % song_plays_path)
    with open(song_plays_path, 'w') as f:
      for user_id in range(0, num_users):
        user_name = 'user-%d' % user_id
        current_time = start_time
        for track_id in range(0, len(track_plays[user_id])):
          current_unixtime = int(time.mktime(current_time.utctimetuple()) * 1000)
          song_id = int(track_plays[user_id][track_id])
          song_key = 'song-%s' % song_id
          song_duration = song_metadata[song_id]['duration']
          f.write('{ "user_id" : "%s", "play_time" : "%d", "song_id" : "%s" }\n'
              % (user_name, current_unixtime, song_key))
          current_time = current_time + datetime.timedelta(seconds=song_duration)
    del user_id, track_id


def main(args):
  global FLAGS
  FLAGS = BuildFlagParser().parse_args(args[1:])
  generator = KijiMusicDataGenerator()
  generator.ConfigureFromFlags(FLAGS)
  generator.Generate()


if __name__ == '__main__':
    main(sys.argv)
