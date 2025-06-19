# Greenhouse JobService

## Overview

This service consumes a message from the Message Broker containing all details needed to run a job and runs the job
execution service.

The job execution strategy is defined by the ContractKey provided in the message.

## Message Example

```json
{
  "ContractKey": "LinkedIn-AggregateImportJob",
  "JobGuid": "C5B5792D-EBDE-47EE-B9CD-C5ABA3963A19",
  "Step": 2,
  "SourceId": 1,
  "IntegrationId": 123,
  "ServerId": 25,
  "TimeZoneString": ""
}
```

## Supported Job Execution Strategies

* LinkedIn-AggregateImportJob

## Getting Started

* Set up a connection to RabbitMQ by providing the connection string to the Configuration path:
  `ConnectionStrings:rabbitmq` and run the project.
* Alternatively, run Greenhouse.Aspire.AppHost (.NET Aspire).

## Containerization

Use the Dockerfile in the root folder to build your image or target your IDE debugger to it.

Pass the following environment variables to the container:
```text
ASPNETCORE_ENVIRONMENT=Development
AWS_PROFILE=DEV
AWS_ACCESS_KEY_ID=<your-key>
AWS_SECRET_ACCESS_KEY=<your-secret>
```

## Using .NET Aspire

### Prerequisites

* Container runtime installed (Docker or Podman)
* .NET Aspire installed

### .NET Aspire installation

#### Installation commands

```bash
dotnet workload update
```

```bash
dotnet workload install aspire
```

#### Check .NET Aspire version

```bash
dotnet workload list
```

## Resources

* [Aspire Overview](https://learn.microsoft.com/en-us/dotnet/aspire/get-started/aspire-overview)
* [Docker](https://www.docker.com/)
* [Podman](https://podman.io/)
* [RabbitMQ](https://www.rabbitmq.com/)



