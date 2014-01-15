Item/Item Collaborative Filtering Example
======================================================

Introduction
------------

In this example, we implement a top-N recommendation system using item-based
collaborative filtering.

Overview of the process:

  * Import the movie database into a kiji table
  * Compute item similarities
  * Get a list of the k nearest neighbors for every item
  * Run the score function
    - Input: An active user and the item in question
    - Get the list of the k nearest neighbors for the item in question (that the user has also
      rated)
    - The score is the sum of similarities * ratings (from the user), divided by the sum of all of
      the similarities
    - Output: A score for this item

Inspired by [Mahout implementation](http://isabel-drost.de/hadoop/slides/collabMahout.pdf)

Build the code
--------------
In the kiji-modeling-examples project, run:

    mvn clean install


Setup
-----
    export MOVIES_HOME=path/to/kiji-modeling/kiji-modeling-examples
    export KIJI_CLASSPATH=${MOVIES_HOME}/target/kiji-modeling-examples-${project.version}.jar

Download the MovieLens 100k dataset from http://grouplens.org/datasets/movielens and unzip, then:

    export MOVIES_RESOURCES=path/to/ml-100k

Install a Kiji instance.

    export KIJI=kiji://.env/item_item_cf
    kiji install --kiji=${KIJI}


Create the tables
-----------------
The `movies.ddl` file contains the DDL instructions for creating three tables:
- `user_ratings` contains all of the ratings that users have assigned to movies.  There is one row
  per user, and we use a map-type column family for the ratings (where the qualifier is the movie ID
  and the value of the column is the score).
- `item_item_similarities` contains the collaborative-filtering model.  For every item in the system,
  this table contains a vector of the top-M most-similar items.  We store these vectors in specific Avro records.
- `movie_titles` maps movie IDs to actual titles.

We create the tables with the following command:

    kiji-schema-shell --kiji=${KIJI} --file=$MOVIES_HOME/src/main/layouts/movielens/movies.ddl

We then make sure that the table actually exists:

    ~MOVIES_HOME $ kiji-schema-shell --kiji=${KIJI}
    Kiji schema shell v1.3.1
    Enter 'help' for instructions (without quotes).
    Enter 'quit' to quit.
    DDL statements must be terminated with a ';'
    schema> show tables;
    13/11/11 20:59:57 WARN org.apache.hadoop.util.NativeCodeLoader: Unable to load native-hadoop library for your platform...
    using builtin-java classes where applicable
    Table                          Description
    ======                         ========================================
    item_item_similarities         Top-M list of similar items, using adjusted cosine similarity
    movie_titles                   movie IDs -> titles
    user_ratings	               User ratings of movies (movies are rows)


Upload data to HDFS
-------------------
Next, we put all of our input data onto HDFS:

    hadoop fs -mkdir item_item_cf
    hadoop fs -copyFromLocal ${MOVIES_RESOURCES}/u.{data,item} item_item_cf/

To check that the data actually got copied:

    ~MOVIES_HOME $ hadoop fs -ls item_item_cf
    13/11/11 21:04:18 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform...
    using builtin-java classes where applicable
    Found 2 items
    -rw-r--r--   3 username supergroup      1979173         2013-12-17 15:00   item_item_cf/u.data
    -rw-r--r--   3 username supergroup       236344         2013-12-17 15:00   item_item_cf/u.item


Import data into the tables
---------------------------

Next we run one job to import the user data into our Kiji table:

    express job target/kiji-modeling-examples-${project.version}.jar \
        org.kiji.modeling.examples.ItemItemCF.MovieImporter \
        --ratings item_item_cf/u.data \
        --table-uri ${KIJI}/user_ratings \
        --hdfs

And another to import the movie titles:

    express job target/kiji-modeling-examples-${project.version}.jar \
        org.kiji.modeling.examples.ItemItemCF.TitlesImporter \
        --titles item_item_cf/u.item \
        --table-uri ${KIJI}/movie_titles \
        --hdfs

And then check that the data actually showed up:

    kiji scan ${KIJI}/user_ratings --max-rows=1
    kiji scan ${KIJI}/movie_titles --max-rows=1


Create the item-item similarity list
------------------------------------

This is where most of the excitement happens.  We compute the cosine similarities between all pairs
of items that have at least one common user who has reviewed them.  We then take the top M
most-similar items for a given item, sort them, and store them as a vector in our
`item_item_similarities` table:

    express job target/kiji-modeling-examples-${project.version}.jar \
        org.kiji.modeling.examples.ItemItemCF.ItemSimilarityCalculator \
        --ratings-table-uri ${KIJI}/user_ratings \
        --similarity-table-uri ${KIJI}/item_item_similarities \
        --model-size 50 \
        --hdfs

This job is quite a beast and can run for a very long time!


Run the scorer
--------------
Here we predict the score for a given user and item by taking a weighted average of all of the
similar items the user has rated.

    express job target/kiji-modeling-examples-${project.version}.jar \
        org.kiji.modeling.examples.ItemItemCF.ItemScorer \
        --ratings-table-uri ${KIJI}/user_ratings \
        --similarity-table-uri ${KIJI}/item_item_similarities \
        --titles-table-uri ${KIJI}/movie_titles \
        --users-and-items 744:132 \
        --k 20 \
        --output-mode run \
        --hdfs

This Express job will write out the predicted scores for the user-item pairs that you provide in a
CSV file on HDFS:

    $ hadoop fs -cat score/part-00000
    13/11/18 11:16:12 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
    744,132,4.0225,"Wizard of Oz, The (1939)"


Run the recommender
-------------------
We recommend items to a user based on what is in his or her shopping cart by taking the sum of all
of similar items across all items in the cart.

    express job target/kiji-modeling-examples-${project.version}.jar \
        org.kiji.modeling.examples.ItemItemCF.ItemRecommender \
        --similarity-table-uri ${KIJI}/item_item_similarities \
        --titles-table-uri ${KIJI}/movie_titles \
        --items 132 \
        --hdfs

Again, the Express job writes the results into a CSV on HDFS:

    $ hadoop fs -cat recommendation/part-00000
    13/11/18 11:19:02 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your
    platform... using builtin-java classes where applicable
    181,0.1692,Return of the Jedi (1983)
    178,0.1700,12 Angry Men (1957)
    657,0.1733,"Manchurian Candidate, The (1962)"
    404,0.1753,Pinocchio (1940)
    197,0.1761,"Graduate, The (1967)"
    127,0.1769,"Godfather, The (1972)"
    143,0.1785,"Sound of Music, The (1965)"
    ...


Debugging tips
--------------
If you need to look at your system logs, try:

    http://localhost:50030/logs/userlogs/