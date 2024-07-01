# What is this for?

This folder should be used to store configuration files used by Kedro or by separate tools.

## Local configuration

The `local` folder should be used for configuration that is either user-specific (e.g. IDE configuration) or protected (e.g. security keys).

## Base configuration

The `base` folder is for shared configuration, such as non-sensitive and project-related configuration that may be shared across team members. Note that **credentials** are ignored by git, in practice users will need to create a `credentials.yml` file in `base` folder to include sensitive information, such as AWS S3 credentials.

## Findings 
- There are many repeated publication outcomes, often with different IDs.
- 