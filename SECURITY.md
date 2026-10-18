# Security policy

## Supported versions

Security fixes go into the latest release on PyPI. Older releases are not patched.

## Reporting a vulnerability

Please do not open a public issue for a security problem.

Report it privately through GitHub: open the repository's **Security** tab and choose **Report a vulnerability**, or go straight to <https://github.com/stevenayers/pytest-databricks-lakeflow/security/advisories/new>. If you cannot use GitHub, email <steven@ayers.io> with "pytest-databricks-lakeflow" in the subject.

Include what the problem is, how to reproduce it and which version is affected. You will get an acknowledgement within a week. Once a fix is released, the advisory is published and credited to you unless you ask otherwise.

## What counts

The plugin runs with your Databricks credentials. It deploys bundles, creates, runs and deletes jobs and schemas, and shells out to the Databricks CLI. Report anything that lets a test do more than its credentials and configuration allow, lets an ephemeral job escape the plugin's naming, tagging and teardown, or leaks credentials into logs, reports or job settings.
