# Base runtime image
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base

# Install ODBC dependencies in the base image
USER root
RUN apt-get update && apt-get install -y \
    unixodbc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Download and install the Amazon Redshift ODBC driver
RUN curl https://s3.amazonaws.com/redshift-downloads/drivers/odbc/2.1.3.0/AmazonRedshiftODBC-64-bit-2.1.3.0.x86_64.rpm -o redshift-odbc.rpm \
    && apt-get update \
    && apt-get install -y alien \
    && alien -i ./redshift-odbc.rpm \
    && rm redshift-odbc.rpm \
    && rm -rf /var/lib/apt/lists/* 

# Copy ODBC configuration
COPY .config/odbc/odbcinst.ini /usr/local/odbc/odbcinst.ini
COPY .config/odbc/amazon.redshiftodbc.ini /etc/amazon.redshiftodbc.ini

# Set ODBC environment variables
ENV ODBCINI=/usr/local/odbc/odbc.ini \
    ODBCSYSINI=/usr/local/odbc \
    AMAZONREDSHIFTODBCINI=/etc/amazon.redshiftodbc.ini
    
# Create files directory, set ownership to non-root user, and grant read/write/execute permissions
RUN mkdir -p /app/files && \
    chown -R $APP_UID:$APP_UID /app/files && \
    chmod 755 /app/files

# Switch back to non-root user
USER $APP_UID
WORKDIR /app
EXPOSE 8080
EXPOSE 8081

# Build image
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["Greenhouse.JobService/Greenhouse.JobService.csproj", "Greenhouse.JobService/"]
RUN dotnet restore "Greenhouse.JobService/Greenhouse.JobService.csproj" --property WarningLevel=0
COPY . .
WORKDIR "/src/Greenhouse.JobService"
RUN dotnet build "Greenhouse.JobService.csproj" --property WarningLevel=0 -c $BUILD_CONFIGURATION -o /app/build

# Publish image
FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "Greenhouse.JobService.csproj" --property WarningLevel=0 -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

# Final image
FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Greenhouse.JobService.dll"]
