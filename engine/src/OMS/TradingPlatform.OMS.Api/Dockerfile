FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY Directory.Build.props .
COPY Directory.Packages.props .
COPY src/TradingPlatform.Shared/ src/TradingPlatform.Shared/
COPY src/OMS/ src/OMS/

RUN dotnet restore src/OMS/TradingPlatform.OMS.Api/TradingPlatform.OMS.Api.csproj
RUN dotnet publish src/OMS/TradingPlatform.OMS.Api/TradingPlatform.OMS.Api.csproj \
    -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app

RUN useradd -r -u 1001 -g root trading
COPY --from=build /app/publish .

USER trading
EXPOSE 8080
ENTRYPOINT ["dotnet", "TradingPlatform.OMS.Api.dll"]
