---
layout: post
title: Lifecycle of a Freshener
categories: [userguides, scoring, 0.9.0]
tags : [scoring-ug]
order : 5
version : devel
description: Description of the lifecycle of a Freshener.
---

<h3 style="margin-top:0px;padding-top:10px;"> Lifecycle of a Freshener </h3>
For this example we will create a Freshener with stock AlwaysFreshen policy and an imaginary `ScoreFunction` implementation called com.mycompany.RecommendingScoreFunction.

First we register the Freshener to a column in our table using the Fresh tool CLI.

    kiji fresh --target=kiji://.env/default/users/derived:recommendations \
        --policy-class=org.kiji.scoring.lib.AlwaysFreshen \
        --score-function-class=com.mycompany.RecommendingScoreFunction \
        --instantiate-classes

This command registers a Freshener to the column 'derived:recommendations' in the table 'users' with the policy class AlwaysFreshen and `ScoreFunction` class RecommendingScoreFunction. The --instantiate-classes flag tells the tool that it should invoke the empty default constructors of both classes and call `serializeToParameters` on the resulting objects during registration. The output of each `serializeToParameters` and any manually specified parameters (here there are none) are merged with key collisions resolved by the following order of precedence: manually specified parameters have highest precedence followed by the policy’s serialized parameters, with the `ScoreFunction`’s serialized parameters last.

Now that our Freshener is registered, `FreshKijiTableReader`s can load it and use it to freshen data. Regular `KijiTableReader` instances will not freshen, regardless of Fresheners attached to requested columns. Opening a `FreshKijiTableReader` configured to freshen 'derived:recommendations' will cause it to load and setup both the policy and `ScoreFunction`. We can open a reader to freshen 'derived:recommendations' as follows:

    final KijiColumnName recsCol = new KijiColumnName("derived", "recommendations");
    final FreshKijiTableReader freshReader = FreshKijiTableReader.Builder.create()
        .withTable(usersTable)
        .withColumnsToFreshen(Lists.newArrayList(recsCol))
        .build();

As part of the initialization of the reader, it will retrieve the Freshener record we made earlier and setup the classes found there. The Freshener record contains the names of the policy and score function classes as well as any serialized parameters provided during registration. Once it has retrieved the record, the reader first invokes the empty default constructors of both classes. With the newly created `KijiFreshnessPolicy` and `ScoreFunction` objects and a `FreshenerGetStoresContext` created from the Freshener record it calls `getRequiredStores` on the policy and `ScoreFunction`. If both classes define a `KeyValueStore` with the same name, the policy’s store will override the `ScoreFunction`’s. The `FreshenerGetStoresContext` is then combined with the newly created `KeyValueStores` to create a `FreshenerSetupContext`, which is passed to the policy and then `ScoreFunction` setup methods. The now setup policy and `ScoreFunction` are cached in the reader to minimize the cost of read requests which they may freshen.

Once all Fresheners have been loaded and setup, the reader is open for requests. When the reader receives a request that includes 'derived:recommendations' the Freshener setup earlier will run. First, the policy’s `shouldUseClientDataRequest` method will be called. The AlwaysFreshen policy's logic is very simple: It always returns false from `isFresh`, regardless of the data, so as an optimization `shouldUseClientDataRequest` returns false. This causes AlwaysFreshen's `getDataRequest` to be called for the `KijiDataRequest` that will be used to read data from the backing Kiji table. In the case of AlwaysFreshen, however, this data request is empty, allowing the system to generate an empty `KijiRowData` without a trip to Kiji.

NOTE: A request to a fresh reader may include parameter overrides which are merged with existing parameters created earlier during registration. These overrides have the highest priority if there are key conflicts.

This empty row data will be passed to `isFresh` which is hard coded to return false every time. Because the policy indicates that data is stale, the `ScoreFunction` will run to refresh the data. First the `ScoreFunction`’s `getDataRequest` method is called and applied to the table to retrieve any data necessary for scoring. In the case of RecommendingScoreFunction this data request may include a user’s purchasing history and other distinguishing information. The row data containing this user information is passed to `score` which uses it to calculate a new recommendation for the user. The `score` method returns this new recommendation and the `FreshKijiTableReader` writes it to the Kiji table and returns it to the client who requested it. Requests like this may run many times and, in the presence of multiple threads, may run concurrently with the same policy and `ScoreFunction` objects.

When a fresh reader is closed, or a Freshener record is changed or removed, loaded Fresheners may be unloaded and cleaned up. The cleanup process involves creating a `FreshenerSetupContext` from the configuration in the Freshener record and calling the policy and `ScoreFunction` cleanup methods with that context.
