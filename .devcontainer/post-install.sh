#!/bin/sh

date >> ~/status
echo "post-install.sh ..." >> ~/status

# copy vscode files
mkdir -p .vscode && cp docs/vscode-template/* .vscode

# run dotnet restore
dotnet restore src/imdb-import.csproj

# source the bashrc-append from the repo
# you can add project specific settings to .bashrc-append and
# they will be added for every user that clones the repo with Codespaces
# including keys or secrets could be a SECURITY RISK
echo "" >> ~/.bashrc
echo ". ${PWD}/.devcontainer/.bashrc-append" >> ~/.bashrc

date >> ~/status
echo "Done" >> ~/status
