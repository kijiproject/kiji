---
layout: post
title: Freshness Policy
categories: [tutorials, scoring, 0.13.0]
tags : [scoring-tutorial]
version: 0.13.0
order : 3
description: Freshness Policy.
---
KijiScoring requires you to provide a "freshness policy", which indicates
whether or not to run scoring on the requested data. It also requires you to provide
a "score function", which is the computation that needs to be performed in real-time
on the data.

KijiScoring provides the following stock freshness policies:

*  **AlwaysFreshen**: returns "stale" for any requested data so that the scoring
   function is always run to freshen the data.
*  **NeverFreshen**: returns "fresh" for any requested data so that the scoring function
   is never needed.
*  **NewerThan**: returns "fresh" if requested data was modified later than a specified
   timestamp.
*  **ShelfLife**: returns "fresh" if requested data was modified within a specified number
   of milliseconds of the current time.

If you choose to use one of the stock policies, you would simply register the stock policy
as described in the next step of the tutorial. If you want to provide a custom policy,
you would implement a new `KijiFreshnessPolicy` instance, as follows.

### Custom KijiFreshnessPolicy Class

<div id="accordion-container">
  <h2 class="accordion-header">org.kiji.scoring.music.NextSongRecommenderFreshnessPolicy</h2>
  <div class="accordion-content">
    <script src="http://gist-it.appspot.com/github/kijiproject/kiji-scoring-music/raw/kiji-scoring-root-0.13.0/src/main/java/org/kiji/scoring/music/NextSongRecommenderFreshnessPolicy.java"> </script>
  </div>
</div>


In this tutorial, we'll use a custom freshness policy that returns "fresh" if we have
generated a recommendation since the last time the user played a track, and "stale"
otherwise. The policy, reproduced above, implements three methods of the
`KijiFreshnessPolicy` interface: `shouldUseClientDataRequest`, `getDataRequest`, and
`isFresh`. The first two ensure that `isFresh` has the data it requires to test for
freshness by checking if the client's data request contains the necessary columns and,
if not, requesting the necessary data to create the `KijiRowData` that is passed to
`isFresh`. After the necessary data is retrieved, `isFresh` checks the timestamp of
the most recent cell written to the play history column ("info:track_plays") against
the timestamp of the most recent cell written to the recommendations column
("info:next_song_rec") to determine whether a new recommendation should be generated.

With this policy, we can avoid generating redundant recommendations, while always
returning the recommendation for the user's most recently played track.

