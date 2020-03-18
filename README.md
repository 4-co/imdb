# Sample IMDb data for use with Cosmos DB

![License](https://img.shields.io/badge/license-MIT-green.svg)
![Docker Image Build](https://github.com/retaildevcrews/imdb/workflows/Docker%20Image%20Build/badge.svg)

This repository contains an extract of 1300 movies and associated actors, producers, directors and genres from the IMDb public data available [here](https://www.imdb.com/interfaces/).

The purpose of this repo is to demonstrate some NoSQL modeling and querying techniques and decisions when using Cosmos DB as a database.

```none

IMDb shares their data for non-commercial use only. Please respect their data policies.

Information courtesy of
IMDb
(http://www.imdb.com)
Used with permission.

```

## Create Cosmos DB Server, Database and Container and load the IMDb sample data

This takes several minutes to run

```bash

# set environment variables

# location
export Imdb_Location="centralus"

# replace xxxx with a unique identifier (or replace the entire name)
# do not use punctuation or uppercase (a-z, 0-9)
export Imdb_Name="imdbcosmosxxxx"

export Imdb_DB="imdb"

export Imdb_Col="movies"

## if true, change name to avoid DNS failure on create
az cosmosdb check-name-exists -n ${Imdb_Name}

# Resource Group Name
export Imdb_RG=${Imdb_Name}-cosmos-rg

# create a new resource group
az group create -n $Imdb_RG -l $Imdb_Location

# create the Cosmos DB server
az cosmosdb create -g $Imdb_RG -n $Imdb_Name

# export readwrite key
export Imdb_Key=$(az cosmosdb keys list -n $Imdb_Name -g $Imdb_RG --query primaryMasterKey -o tsv)

# create the database
az cosmosdb sql database create -a $Imdb_Name -n $Imdb_DB -g $Imdb_RG

# create the container
# 400 is the minimum RUs
# /partitionKey is the partition key
# partiton key is the id mod 10
az cosmosdb sql container create --throughput "400" -p /partitionKey -g $Imdb_RG -a $Imdb_Name -d $Imdb_DB -n $Imdb_Col --idx @index.json

### Note: the az cosmosdb sql command set is still in preview
###       verify that the results of the above command show two composite indices created
###       If not, run the command below (you can ignore the deprecated warning)
az cosmosdb collection update -g $Imdb_RG -n $Imdb_Name -d $Imdb_DB -c $Imdb_Col --indexing-policy @index.json


# run the docker IMDb Import app
docker run -it --rm retaildevcrew/imdb-import $Imdb_Name $Imdb_Key $Imdb_DB $Imdb_Col

```

## Exploring the data

* Open Azure Portal and navigate to the Cosmos DB blade created above
* Select Data Explorer and open the container to see the data loaded

## Design Decisions

In considering the design, we wanted to follow document design best practices as well as optimizing for this specific problem.

## One container

We chose to include different document types in the same container for simplicity (and to demonstrate). You can read about some of the tradeoffs [here](https://docs.microsoft.com/en-us/azure/cosmos-db/modeling-data) and see some of the side effects in the queries below. Note that some frameworks (like Spring Boot) require a separate container for each document type.

Each document has a type field that is one of: Movie, Actor or Genre

ID has to be unique, so we use movieId, actorId or genre as the ID. Reading by ID is the fastest (and cheapest) way to retrieve a document.

## Partitioning Strategy

The Cosmos DB partition key used is /partitionKey and is computed by taking the integer portion of movieId or actorId mod 10 and converting to a string which results in 10 partitions ("0" - "9")

Note: the partition key must be a string

Note: Genres use a partitionKey of "0" as there are only 19 Genres

You want your partition key to be well distributed from a storage and usage perspective. For Actors, a good partition key could be birthYear mod x. However, this would likely not be a good partition key for Movies as a high percentage of the requests are likely to be for the current year which would create a hot partition. A hash of the title would likely be a good choice. movieId (and actorId) are integers with a character preface (tt or nm), so a mod x on the integer portion is a good choice as well and the one we chose.

In order to use the Cosmos DB API to read a single 1K document using 1 RU, you need to know the partition key, so having a value that you can compute the partition key from is a best practice. Note that some frameworks (like Spring Boot) don't support the single document read API and always use the query API. This can have a significant cost impact depending on the access pattern.

You want to avoid cross-partition queries when possible as they incur additional work which increases the RUs and cost.

Read more about partitioning strategies [here](https://docs.microsoft.com/en-us/azure/cosmos-db/partition-data)

## Fast Changing Data

Generally, you don't want to combine fast changing data and slow changing data in the same document. In this example, "ratings" is a summary measure that would be periodically updated by a batch process. Because the data updates are known and bounded and the document is small, we chose to combine for ease of use. More information [here](https://docs.microsoft.com/en-us/azure/cosmos-db/modeling-data)

## Embedded Links

Movies have actors (and producers and directors and crew ...) and Actors star in Movies.

In a relational model, you would normally have a "MoviesActors" table and join. In a document model, you normally embed unless the embedded data is fast changing or potentially grows to be very large. More information [here](https://docs.microsoft.com/en-us/azure/cosmos-db/modeling-data)

A common usage for this data would be to show the Actor and what movies they were in (or a Movie and the Actors in it). If you embed just the ID, this would be two serial queries. One to get the Actor and the Movie IDs and a second one to pull back the movie information. Given the size of the documents, we chose to optimize this by embedded the entire Movie into the Actor document (and Actors into the Movie document). This simplifies reads but complicate writes. In a high read situation (like showing movies on a web site), this is a good optimization. Just keep an eye on document size and update frequency / complexity.

Note: When you update a single field in a document, Cosmos DB writes the entire document which can change your IO requirements compared to a relational DBMS.

A good example of what you would not want to embed is the individual ratings. Some movies have over 100K ratings, so you would want to keep the individual ratings in a separate container and have a process that summarizes and updates the aggregate every n minutes.

## Searching

Some of the sample queries search the Movie Title or Actor Name using a "like" query. For a small amount of documents searching across a small number of fields, this works fine. However, if search is a primary use case or you want "full text" search, you should integrate Cosmos DB with Azure Search as the queries will be richer, faster and less expensive.

The Genre search uses an "array_contains" search. In a relational model, you would likely have a MoviesGenres table and use a join (a Movie has 1..n Genres)

Note that in Cosmos DB, search is case sensitive, so searching Movies for "matrix" will return zero documents but searching for "Matrix" will return the correct number of documents. We chose to address this by adding a "textSearch" field that is a lowercase version of the title or actor name (it could contain other keywords as well). This adds size to the document, but prevents having to use contains(lower(m.title), 'matrix'). By using lower(), you are doing a full table scan and not using the index. For 1300 movies, this is minimal work, but at scale, it will be slow and expensive.

Order by is also case sensitive in Cosmos DB, so Alice Through the Looking Glass sorts before Alice in Wonderland. We use the textSearch field to avoid this behavior. We also create composite indices on textSearch, movieId for Movies and textSearch, actorId for Actors. Since Movies and Actors may have the same name, this allows us to ensure deterministic ordering. To order by a composite key in Cosmos DB, you must first create the composite index. See [index.json](./index.json) for the index definitions.

Again, for large or advanced search workloads, you should integrate Azure Search (or SOLR) as part of the solution, using the Cosmos DB [change feed](https://docs.microsoft.com/en-us/azure/cosmos-db/change-feed) to keep the index fresh is a proven model.

## Understanding RUs

A best practice is to baseline the RUs for each "action" and include as part of your testing suite. Changes to your document model or query can result in significant changes in RUs. The Cosmos DB API has the ability to capture RUs for each action, so building a baseline is straight forward.

General best practices like limiting the columns selected, limiting the documents selected and avoiding table scans are important. The deeper a filter condition is in the document model, the more work the query processor has to do (and the more RUs it consumes), so keep frequent predicates at the root whenever possible and/or use indexing [policies](https://docs.microsoft.com/en-us/azure/cosmos-db/index-policy) to optimize common queries.

Avoid cross partition queries when possible. Cosmos DB will run the query in parallel, but it is more work and thus higher RUs.

## Key-Value Cache

Cosmos DB is an excellent key-value cache with simple geo-distribution and replication. Performance is often better than other caching solutions and Cosmos DB is cost competitive. The added simplicity of having one data access API and one data platform to manage makes development and operations more efficient.

Some general guidelines:

* Use the native (SQL) API
* Use a separate container for your key-value cache than your operational data
* Use an efficient partition hash that distributes storage and access evenly (int mod x works well for numeric keys)
* Use indexing [policies](https://docs.microsoft.com/en-us/azure/cosmos-db/index-policy) to turn off indexing for the value in a key-value store
* Use direct access by ID and partition key for single document reads
* Use Cosmos DB [TTL](https://docs.microsoft.com/en-us/azure/cosmos-db/time-to-live) to automatically remove old items
* Use Cosmos DB [change feed](https://docs.microsoft.com/en-us/azure/cosmos-db/change-feed) to extract values into other systems

## Conclusion

Unlike relational modeling where specific normal forms are verifiable, document modeling is a collection of decisions based heavily on usage patterns. There is no "right" or "correct" answer but there are best practices and trade offs based on usage. It is important to understand the usage patterns early so that you can optimize the document model.

## Sample Queries

Click the new sample query icon in the Data Explorer tool bar and run the default select * query to see the first 100 documents

Cosmos DB Query [cheat sheet:](<https://docs.microsoft.com/en-us/azure/cosmos-db/query-cheat-sheet>)

```sql

# Simplest query
select * from m

# List of movies
select m.movieId, m.type, m.title, m.year, m.runtime, m.genres, m.roles
from m
where m.type = 'Movie'
order by m.textSearch, m.movieId

# List of Genres
select m.genre
from m
where m.type = 'Genre'
order by m.genre

# List of Actors
select m.actorId, m.type, m.name, m.birthYear, m.deathYear, m.profession, m.movies
from m
where m.type = 'Actor'
order by m.textSearch, m.actorId

# Simple transform
select value m.title
from m
where m.type = 'Movie'

# Unexpected behavior
# This is a side effect of combining the document types in one container
select m.title
from m

# Info about a great movie
select m.movieId, m.type, m.rating, m.votes, m.title, m.year, m.runtime, m.genres, m.roles
from m
where m.id = 'tt0133093'

# A list of specific movies
select m.movieId, m.type, m.rating, m.votes, m.title, m.year, m.runtime, m.genres, m.roles
from m
where m.movieId in ('tt0167260', 'tt0419781', 'tt0367495', 'tt0120737', 'tt0358456')
order by m.textSearch, m.movieId

# The API has a more efficient way to retrieve exactly one document by ID
#   It is faster and consumes less RUs and should be used in most scenarios
# However, when retrieving 4 or more movies (in this data set), it is less RUs
#   to use a query than single reads and is also easier to use
#   this will vary slightly by model and data size. Single reads are constant.

# An actor from a great movie
select m.actorId, m.type, m.name, m.birthYear, m.deathYear, m.profession, m.movies
from m
where m.id = 'nm0000206'

# Movies Jennifer Connelly is in
select m.movieId, m.type, m.rating, m.votes, m.title, m.year, m.runtime, m.genres, m.roles
from m
where array_contains(m.roles, { actorId: 'nm0000124' }, true)
order by m.textSearch, m.movieId

# Another way
# note you can't use select * or select m.*
select m.movieId, m.type, m.title, m.year, m.runtime, m.genres, m.roles
from movies m
join r in m.roles
where r.actorId = 'nm0000124'
order by m.textSearch, m.movieId

# Action Movies
# Note this is case sensitive, so 'action' won't work
select m.movieId, m.type, m.rating, m.votes, m.title, m.year, m.runtime, m.genres, m.roles
from m
where array_contains(m.genres, 'Action')
order by m.textSearch, m.movieId

# Search movie title for 'rings'
select m.movieId, m.type, m.rating, m.votes, m.title, m.year, m.runtime, m.genres, m.roles
from m
where contains(m.textSearch, 'rings')
order by m.textSearch, m.movieId

# Long movies
select top 5 m.movieId, m.type, m.rating, m.votes, m.title, m.year, m.runtime, m.genres, m.roles
from m
order by m.runtime desc

# Highest rated movies
select top 5 m.movieId, m.type, m.rating, m.votes, m.title, m.year, m.runtime, m.genres, m.roles
from m
order by m.rating desc

# Movies by year
select m.movieId, m.type, m.rating, m.votes, m.title, m.year, m.runtime, m.genres, m.roles
from m
where m.year = 2006
order by m.textSearch, m.movieId

# Search actor names for 'tom'
select m.actorId, m.type, m.name, m.birthYear, m.deathYear, m.profession, m.movies
from m
where contains(m.textSearch, 'tom')
order by m.textSearch, m.actorId

# Actors in more than one movie
select m.actorId, m.type, m.name, m.birthYear, m.deathYear, m.profession, m.movies
from m
where array_length(m.movies) > 1
order by m.textSearch, m.actorId

```
