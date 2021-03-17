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

> Visual Studio Codespaces is the easiest way to evaluate the IMDb data as all of the prerequisites are automatically installed
>
> Follow the setup steps in the [Helium readme](https://github.com/retaildevcrews/helium) to setup Codespaces

## Prerequisites

- Bash shell (tested on Visual Studio Codespaces, Cloud Shell, Mac, Ubuntu, Windows with WSL2)
  - Will not work with WSL1
- Docker ([download](https://www.docker.com/products/docker-desktop))
- .NET Core SDK 3.x (if not using Docker) ([download](https://dotnet.microsoft.com/download))
- Azure CLI ([download](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli?view=azure-cli-latest))
- Visual Studio Code (optional) ([download](https://code.visualstudio.com/download))

## Create Cosmos DB Server, Database and Container

> The `az cosmosdb sql` extension is currently in preview and is subject to change

```bash

# replace with a unique name
# do not use punctuation or uppercase (a-z, 0-9)
export Imdb_Name={your Cosmos DB name}

## if true, change name to avoid DNS failure on create
az cosmosdb check-name-exists -n ${Imdb_Name}

# set environment variables
export Imdb_Location="centralus"
export Imdb_DB="imdb"
export Imdb_Col="movies"

```

> This environment variable saves the command as a string instead of running the command and saving the output. This is intentional to avoid saving sensitive data in environment variables. Make sure to run the export commands as is. When the value is needed, the command will be executed with `eval`. For example, `dotnet run -- $Imdb_Name $(eval $Imdb_RW_Key) ...`.

```bash

export Imdb_RW_Key='az cosmosdb keys list -n $Imdb_Name -g $Imdb_RG --query primaryMasterKey -o tsv'

```

```bash

# Resource Group Name
export Imdb_RG=${Imdb_Name}-cosmos-rg

# create a new resource group
az group create -n $Imdb_RG -l $Imdb_Location

# create the Cosmos DB server
# this command takes several minutes to run
az cosmosdb create -g $Imdb_RG -n $Imdb_Name

# create the database
# 400 is the minimum --throughput (RUs)
az cosmosdb sql database create -a $Imdb_Name -n $Imdb_DB -g $Imdb_RG --throughput 1000

# create the container
# /partitionKey is the partition key (case sensitive)
az cosmosdb sql container create -p /partitionKey -g $Imdb_RG -a $Imdb_Name -d $Imdb_DB -n $Imdb_Col

```

## Load IMDb sample data

### Option 1: Load data using Docker

```bash

# run the IMDb Import app from Docker
docker run -it --rm retaildevcrew/imdb-import $Imdb_Name $(eval $Imdb_RW_Key) $Imdb_DB $Imdb_Col

```

### Option 2: Load data using .NET Core

```bash

# clone the repo locally
git clone https://github.com/retaildevcrews/imdb imdb

# change to the src directory
cd imdb/src

# run the IMDb Import app from dotnet
dotnet run -- $Imdb_Name $(eval $Imdb_RW_Key) $Imdb_DB $Imdb_Col

```

## Exploring the data

- Open Azure Portal and navigate to the Cosmos DB blade created above
- Select Data Explorer and open the container to see the data loaded

## Design Decisions

In considering the design, we wanted to follow document design best practices as well as optimizing for this specific problem.

## One container

We chose to include different document types in the same container for simplicity (and to demonstrate). You can read about some of the tradeoffs [here](https://docs.microsoft.com/en-us/azure/cosmos-db/modeling-data) and see some of the side effects in the queries below. Note that some frameworks (e.g., Spring Data JPA Repositories) require a separate container for each document type.

Each document has a type field that is one of: Movie, Actor or Genre

ID has to be unique, so we use movieId, actorId or genre as the ID. Reading by ID is the fastest (and least expensive) way to retrieve a document.

## Partitioning Strategy

The Cosmos DB partition key used is /partitionKey and is computed by taking the integer portion of movieId or actorId mod 10 and converting to a string which results in 10 partitions ("0" - "9")

> The partition key must be a string
>
> Genres use a partitionKey of "0" as there are only 19 Genres

Your partition key should be well distributed from a storage and usage perspective. For Actors, a good partition key could be birthYear mod x. However, this would likely not be a good partition key for Movies as a high percentage of the requests are likely to be for the current year which would create a hot partition. A hash of the title would likely be a good choice. The elements movieId (and actorId) are integers with a character preface (tt or nm) which means a mod x on the integer portion is a good choice as well and is the partition key we chose.

In order to use the Cosmos DB API to read a single 1K document using 1 RU you need to know the partition key. So, having a value that you can compute the partition key from is a best practice. Note that some frameworks (e.g., Spring Data JPA Repositories) don't support the single document read API and always use the query API. This can impact cost significantly depending on the access pattern.

You want to avoid cross-partition queries when possible as they incur additional work which increases the RUs and cost.

Read more about partitioning strategies [here](https://docs.microsoft.com/en-us/azure/cosmos-db/partition-data)

## Fast Changing Data

Generally, you don't want to combine fast changing data and slow changing data in the same document. In this example, "ratings" is a summary measure that would be periodically updated by a batch process. Because the data updates are known and bounded and the document is small, we chose to combine for ease of use. More information [here](https://docs.microsoft.com/en-us/azure/cosmos-db/modeling-data)

## Embedded Links

Movies have actors (and producers and directors and crew ...) and Actors star in Movies.

In a relational model, you would normally have a "MoviesActors" table and join. In a document model, you normally embed unless the embedded data is fast changing or potentially grows to be very large. More information [here](https://docs.microsoft.com/en-us/azure/cosmos-db/modeling-data)

A common usage for this data would be to retrieve the Actor and the movies in which they played a role (or a Movie and the Actors in it). Embedding only the movie ID in the Actor document would require two sequential queries. The first query would retrieve the Actor and the Movie IDs and a second one to retrieve the movie information. Given the size of the documents, we chose to optimize the data structure for these queries by including key Movie fields in the Actor document and, similarly, Actors into the Movie document. This simplifies reads, but complicate writes. In a high read situation (e.g., showing movies on a web site), this is a good optimization. As you optimize be sure to monitor document size and update frequency and complexity.

> When you update a single field in a document, Cosmos DB writes the entire document which can change your IO requirements compared to a relational DBMS.

A good example of what you would not want to embed is the individual ratings. Some movies have over 100K ratings, so you would want to keep the individual ratings in a separate container and have a process that summarizes and updates the aggregate every n minutes.

## Searching

Some of the sample queries search the Movie Title or Actor Name using a `contains` query. For a small amount of documents searching across a small number of fields, this works well. However, if search is a primary use case or you want "full text" search, you should integrate Cosmos DB with [Azure Cognitive Search](https://docs.microsoft.com/en-us/azure/search/search-howto-index-cosmosdb) as the queries will be richer, faster and less expensive.

The Genre search filters query results by matching an array of Genres within a movie. In a relational model, you would likely have a MoviesGenres table and use a join (a Movie has 1..n Genres). As an optimization, we created the genreSearch field which is a | delimited string of the Genres array. The `array_contains` function is case sensitive and can be costly. By using the `contains` function against the genreSearch field the search is optimized for performance and cost. With the recent [improvement](https://devblogs.microsoft.com/cosmosdb/new-string-function-performance-improvements-and-case-insensitive-search/) in Cosmos DB string functions, we saw a 29% performance improvement and a 5% RU (cost) reduction.

Order by is case sensitive in Cosmos DB, so sorting Movies by title will result in "Alice Through the Looking Glass" appearing before "Alice in Wonderland". We chose to address this by adding a "textSearch" field that is a lowercase version of the title or actor name. This adds size to the document, but ensures results are ordered as expected.

We also create two composite indices with textSearch using movieId for Movies and actorId for Actors. Since Movies and Actors may have the same name using a composite index ensures deterministic ordering. See [index.json](./index.json) for the index definitions.

## Understanding RUs

A best practice is to baseline the RUs for each "action" and include as part of your testing suite. Changes to your document model or query can result in significant changes in RU usage. The Cosmos DB API has the ability to capture RUs for each action, so building a baseline is straight forward.

General best practices like limiting the columns selected, limiting the documents selected and avoiding table scans are important. The deeper a filter condition is in the document model, the more work the query processor has to do (and the more RUs it consumes), so keep frequent predicates at the root whenever possible and/or use indexing [policies](https://docs.microsoft.com/en-us/azure/cosmos-db/index-policy) to optimize common queries.

Avoid cross partition queries when possible. Cosmos DB will run the query in parallel, but it is more work and thus higher RUs.

## Key-Value Cache

Cosmos DB is an excellent key-value cache with simple geo-distribution and replication. Performance is often better than other caching solutions and Cosmos DB is cost competitive. The added simplicity of having one data access API and one data platform to manage makes development and operations more efficient.

Some general guidelines:

- Use the native (SQL) API
- Use a separate container for your key-value cache than your operational data
- Use an efficient partition hash that distributes storage and access evenly (int mod x works well for numeric keys)
- Use indexing [policies](https://docs.microsoft.com/en-us/azure/cosmos-db/index-policy) to turn off indexing for the values in a key-value store
- Use direct access by ID and partition key for single document reads
- Use Cosmos DB [TTL](https://docs.microsoft.com/en-us/azure/cosmos-db/time-to-live) to automatically remove old items
- Use Cosmos DB [change feed](https://docs.microsoft.com/en-us/azure/cosmos-db/change-feed) to extract values into other systems

## Conclusion

Unlike relational modeling where specific normal forms are verifiable, document modeling is a collection of decisions based heavily on usage patterns. There is not a definitively correct answer, but there are best practices and trade-offs based on usage. It is important to understand the usage patterns early so that you can optimize the document model.

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

# Simple transform
# this returns an array of string
select value m.genre
from m
where m.type = 'Genre'
order by m.genre

# List of Actors
select m.actorId, m.type, m.name, m.birthYear, m.deathYear, m.profession, m.movies
from m
where m.type = 'Actor'
order by m.textSearch, m.actorId

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
where m.type = 'Movie'
and array_contains(m.roles, { actorId: 'nm0000124' }, true)
order by m.textSearch, m.movieId

# Another way
# note you cannot use select * or select m.*
select m.movieId, m.type, m.title, m.year, m.runtime, m.genres, m.roles
from movies m
join r in m.roles
where r.actorId = 'nm0000124'
order by m.textSearch, m.movieId

# Action Movies
# This query uses the genreSearch field discussed above
select m.movieId, m.type, m.rating, m.votes, m.title, m.year, m.runtime, m.genres, m.roles
from m
where m.type = 'Movie'
and contains(m.genreSearch, 'Action', true)
order by m.textSearch, m.movieId

# Search movie title for 'Rings'
select m.movieId, m.type, m.rating, m.votes, m.title, m.year, m.runtime, m.genres, m.roles
from m
where m.type = 'Movie'
and contains(m.title, 'Rings', true)
order by m.textSearch, m.movieId

# Long movies
select top 5 m.movieId, m.type, m.rating, m.votes, m.title, m.year, m.runtime, m.genres, m.roles
from m
where m.type = 'Movie'
order by m.runtime desc

# Highest rated movies
select top 5 m.movieId, m.type, m.rating, m.votes, m.title, m.year, m.runtime, m.genres, m.roles
from m
where m.type = 'Movie'
order by m.rating desc

# Movies by year
select m.movieId, m.type, m.rating, m.votes, m.title, m.year, m.runtime, m.genres, m.roles
from m
where m.type = 'Movie'
and m.year = 2006
order by m.textSearch, m.movieId

# Search actor names for 'Tom'
select m.actorId, m.type, m.name, m.birthYear, m.deathYear, m.profession, m.movies
from m
where m.type = 'Actor'
and contains(m.name, 'Tom', true)
order by m.textSearch, m.actorId

# Actors in more than one movie
select m.actorId, m.type, m.name, m.birthYear, m.deathYear, m.profession, m.movies
from m
where m.type = 'Actor'
and array_length(m.movies) > 1
order by m.textSearch, m.actorId

```

## Contributing

This project welcomes contributions and suggestions. Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit [Microsoft Contributor License Agreement](https://cla.opensource.microsoft.com).

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
