FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY RedisPatterns.sln Directory.Build.props ./
COPY src/RedisPatterns.Core/RedisPatterns.Core.csproj src/RedisPatterns.Core/
COPY src/RedisPatterns.Api/RedisPatterns.Api.csproj src/RedisPatterns.Api/
RUN dotnet restore src/RedisPatterns.Api/RedisPatterns.Api.csproj

COPY src/ src/
RUN dotnet publish src/RedisPatterns.Api/RedisPatterns.Api.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app
COPY --from=build /app .
EXPOSE 8080
USER $APP_UID
ENTRYPOINT ["dotnet", "RedisPatterns.Api.dll"]
