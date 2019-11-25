### Build and Unit Test the App
FROM mcr.microsoft.com/dotnet/core/sdk:2.2 AS build


### copy the source
COPY src src

WORKDIR /src

### Build the app
RUN dotnet publish -c Release -o /app


###########################################################

### Build the runtime container
FROM mcr.microsoft.com/dotnet/core/aspnet:2.2 AS runtime

### create a user
RUN groupadd -g 4120 imdb && \
    useradd -r  -u 4120 -g imdb imdb && \
    mkdir -p /home/imdb && \
    chown -R imdb:imdb /home/imdb

### run as imdb user
USER imdb

### copy the data
COPY data /data

### copy the app
COPY --from=build /app /app

WORKDIR /app

ENTRYPOINT [ "dotnet",  "imdb-import.dll" ]
