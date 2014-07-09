---
layout: post
title: Registering a FreshnessPolicy and ScoreFunction
categories: [tutorials, scoring, 0.16.0]
tags : [scoring-tutorial]
version: 0.16.0
order : 5
description: Registering a FreshnessPolicy and ScoreFunction.
---
For KijiScoring to freshen data, it needs to know when data needs to be refreshed
(the freshness policy) and how to refresh the data (the score function). KijiScoring
includes the `fresh` command-line tool to register a policy and score function with a
given Kiji table column.

We'll use the freshness policy and score function created in the previous steps.

To install the provided policy and score function, run the following command:

<div class="userinput">
{% highlight bash %}
kiji fresh \
${KIJI}/users/info:next_song_rec \
--do=register \
--policy-class=org.kiji.scoring.music.NextSongRecommenderFreshnessPolicy \
--score-function-class=org.kiji.scoring.music.NextSongRecommenderScoreFunction \
--parameters={"org.kiji.scoring.music.NextSongRecommenderScoreFunction.kvstore_name":"nextPlayed","org.kiji.scoring.music.NextSongRecommenderScoreFunction.kvstore_table_uri":"kiji://.env/kiji_music/songs","org.kiji.scoring.music.NextSongRecommenderScoreFunction.kvstore_column":"info:top_next_songs"}
{% endhighlight %}
</div>

where `kiji://.env/kiji_music/songs` is the Kiji URI of the songs table from the
[Music Recommendation Tutorial]({{site.tutorial_music_devel}}/music-overview/); you'll
need to change it if you put your Kiji tables in
a different place than described.

You should see this output:

    Freshener attached to column: 'info:next_song_rec' in table: 'users'
    KijiFreshnessPolicy class: 'org.kiji.scoring.music.NextSongRecommenderFreshnessPolicy'
    ScoreFunction class: 'org.kiji.scoring.music.NextSongRecommenderScoreFunction'
    Parameters: {"org.kiji.scoring.music.NextSongRecommenderScoreFunction.kvstore_name":"nextPlayed",
        "org.kiji.scoring.music.NextSongRecommenderScoreFunction.kvstore_table_uri":
                "kiji://.env/kiji_music/songs",
        "org.kiji.scoring.music.NextSongRecommenderScoreFunction.kvstore_column":"info:top_next_songs"}

