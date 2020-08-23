FROM mcr.microsoft.com/dotnet/core/sdk:3.1.201 AS build
WORKDIR .
COPY . .

RUN dotnet build
RUN dotnet test
