---
layout: post
title: Score Function
categories: [tutorials, scoring, 0.15.0]
tags : [scoring-tutorial]
version: 0.15.0
order : 4
description: Score Function.
---
KijiScoring provides an interface for implementing custom model code called `ScoreFunction`.
This interface defines a number of helper functions that enable the main method, `score`,
to operate on data from an entity and produce a score from that data.

### Custom ScoreFunction class

<div id="accordion-container">
  <h2 class="accordion-header">org.kiji.scoring.music.NextSongRecommenderScoreFunction</h2>
  <div class="accordion-content">
    <script src="http://gist-it.appspot.com/github/kijiproject/kiji-scoring-music/raw/kiji-scoring-root-0.15.0/src/main/java/org/kiji/scoring/music/NextSongRecommenderScoreFunction.java"> </script>
  </div>
</div>

This tutorial uses a custom `ScoreFunction` implementation to generate song
recommendations. Our custom `ScoreFunction` may look familiar to those who have
completed the [Music Recommendation Tutorial]({{site.tutorial_music_devel}}/music-overview/)
because we use the same basic logic as the
KijiProducer implementation provided there to generate recommendations. The KijiScoring
version is rewritten to fit the particular constraints of real time scoring.

`NextSongRecommenderScoreFunction` uses a `KeyValueStore` to access precalculated
song to song recommendations so that we can recommend the user listen to the song most
often played after their most recently played track. This logic is contained in the
recommend method which gets the most popular songs played after the user's most
recent play, gets the most popular among them, and returns that song as a recommendation.
