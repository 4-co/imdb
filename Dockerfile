### Build and Unit Test the App
FROM mcr.microsoft.com/dotnet/core/sdk:3.1 AS build


### Copy the source
COPY src src

WORKDIR /src

### Build the app
RUN dotnet publish -c Release -o /app


###########################################################

### Build the runtime container
FROM mcr.microsoft.com/dotnet/core/aspnet:3.1 AS runtime

### copy the data
COPY data /data

### copy the app
COPY --from=build /app /app

WORKDIR /app

### create a user
RUN groupadd -g 4120 imdb && \
    useradd -r  -u 4120 -g imdb imdb && \
    mkdir -p /home/imdb && \
    chown -R imdb:imdb /home/imdb && \
    chown -R imdb:imdb /app && \
    chown -R imdb:imdb /data

### run as imdb user
USER imdb

ENTRYPOINT [ "dotnet",  "imdb-import.dll" ]
