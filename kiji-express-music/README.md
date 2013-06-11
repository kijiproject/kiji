KijiExpress Tutorial
====================

KijiExpress provides a simple data analysis language using
[KijiSchema](https://github.com/kijiproject/kiji-schema/) and
[Scalding](https://github.com/twitter/scalding/).

You can access the full text of the tutorial 
[here](http://docs.kiji.org/tutorials/express-recommendation/0.1.0/music-overview/).

In this tutorial, you will learn to create a simple music recommender. To this end, you will
learn to:

* Install and create Kiji tables
* Import data into them, specifically, metadata for songs and users' listening history.
* Calculate the most popular songs played after a given song (for each song in the data).


Download & Start Bento
----------------------
Download the BentoBox, a development environment with Hadoop, HBase and all Kiji components
from [here](http://archive.kiji.org/tarballs/kiji-bento-albacore-1.0.1-release.tar.gz).

    tar xzf kiji-bento-*.tar.gz

    cd kiji-bento-albacore
    source bin/kiji-env.sh
    bento start


Setup
------
    export MUSIC_EXPRESS_HOME = <path/to/tutorial/root/dir>

Install a Kiji instance.

    export KIJI=kiji://.env/kiji_music
    kiji install --kiji=${KIJI}

Export libs.

    export LIBS_DIR=$MUSIC_EXPRESS_HOME/lib
    export KIJI_CLASSPATH="${LIBS_DIR}/*"


Create the Kiji music tables
----------------------------

The layouts for the tables are described in `music_schema.ddl`.

    kiji-schema-shell --kiji=${KIJI} --file=$MUSIC_EXPRESS_HOME/music_schema.ddl


Upload data to HDFS
-------------------

    hadoop fs -mkdir express-tutorial
    hadoop fs -copyFromLocal $MUSIC_EXPRESS_HOME/example_data/*.json express-tutorial/


Import data into the tables
---------------------------

Import the song metadata into a `songs` table.

    express job kiji-express-music-${project.version}.jar \
    org.kiji.express.music.SongMetadataImporter \
    --input express-tutorial/song-metadata.json \
    --table-uri ${KIJI}/songs --hdfs

Import the users' listening history into a `users` table.

    express job target/kiji-express-music-${project.version}.jar \
    org.kiji.express.music.SongPlaysImporter \
    --input express-tutorial/song-plays.json \
    --table-uri ${KIJI}/users --hdfs


Count the number of times a song was played
-------------------------------------------

This MapReduce job uses the listening history of our users that we have stored in the `users` Kiji
table to calculate the total number of times each song has been played. The result of this computation
is written to a text file in HDFS.

    express job kiji-express-music-${project.version}.jar \
    org.kiji.express.music.SongPlayCounter \
    --table-uri ${KIJI}/users \
    --output express-tutorial/songcount-output --hdfs


Find the top next songs
-----------------------

Now, for each song, we want to compute a list of the songs that most frequently follow that song.
This kind of model can eventually be used to write a song recommender.

    express job kiji-express-music-${project.version}.jar \
    org.kiji.express.music.TopNextSongs \
    --users-table ${KIJI}/users \
    --songs-table ${KIJI}/songs --hdfs


Stop the BentoBox
-----------------

    bento stop
