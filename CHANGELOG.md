# Changelog

All notable changes to this project are recorded here. The file is maintained by
[release-please](https://github.com/googleapis/release-please) from conventional commit messages.

## [0.3.0](https://github.com/stevenayers/pytest-databricks-lakeflow/compare/v0.2.1...v0.3.0) (2026-10-19)


### Features

* add raise_for_failure, root cause, error code and json output to run results ([#76](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/76)) ([0ec51a8](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/0ec51a8a753518c27c95f10c8d568a5687de2ee6))
* add single_node to run the copy's job clusters on one node ([#96](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/96)) ([38e5b3c](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/38e5b3c0e674e9fb60258506a141ca0dbfc53c31))
* add test_marker to declare a __pytest_test_id job parameter on every copy ([#99](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/99)) ([1094fb1](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/1094fb1ec71fcf32ffb6a593cc29f7aca73ddb1a))
* build run.task(key).child_run as a JobRun for run job tasks ([#97](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/97)) ([425141f](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/425141f3b25b24a296084d791ce3854c5d5945ba))
* expose condition outcomes and branch states on task and job runs ([#87](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/87)) ([02dabe2](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/02dabe23c00674133b7420d5790a7ca3f97c1706))
* expose resolved job and task parameters on run results ([#78](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/78)) ([ed7a5fc](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/ed7a5fcfa3220e124bac429a435dbef068c46171))
* expose sql, alert, run-job, dbt, dashboard and clean room task outputs ([#90](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/90)) ([54ff46f](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/54ff46f9ae99f2d6e8cbce36c133557c2f766023))
* expose the timing breakdown and effective performance target ([#86](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/86)) ([33909f2](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/33909f2aa1a0e930c0545c1da5b46f4e6e8278d5))
* fail clearly on a cli and sdk host mismatch and pass bundle variables ([#91](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/91)) ([3964df3](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/3964df3372ac39e5b81be18a46584b8cb5dfe1ff))
* log task state changes while a run is in flight ([#88](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/88)) ([8f27f2b](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/8f27f2b92ca598c492ce386b0be8b906cd61bf29))
* make TaskError.root_cause follow Python chained exceptions ([#93](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/93)) ([e73853a](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/e73853a5dfe7a8b118696e97aa99d9d55b8d10ba))
* mirror the timeout on the job and expose attempts and timed_out ([#82](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/82)) ([0fea9df](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/0fea9df104e59d5ead933f9fe393217b489eb86e))
* pass extra bundle deploy flags and document per-branch test deployments ([#89](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/89)) ([ea5d5e1](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/ea5d5e101a68e74740b1702cc94fcdfca7075669))
* pass pipeline refresh options through job.run ([#94](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/94)) ([4c0b386](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/4c0b3862dc4dbe0f536dc20ab93bb8478b7fe550))
* pause subscriptions in ephemeral jobs and let tests disable tasks ([#84](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/84)) ([ee71149](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/ee71149d6756e6353f6baf719e556e01753c3000))
* send an idempotency token, performance_target and queue with run_now ([#75](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/75)) ([257a1cd](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/257a1cddb9a9ebe0c7373349bd345b0beb564ce1))
* set permissions on ephemeral jobs and allow run_as to be kept or named ([#79](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/79)) ([f3d61d3](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/f3d61d39d85866334f6873a810243ddeccb3420a))
* show resolved parameters in failure messages and the lakeflow section ([#98](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/98)) ([b9eb4cc](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/b9eb4cc752a0470418c3f9cf46fc1513d21c59d5))
* tag ephemeral jobs with ci url, user, commit and branch ([#80](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/80)) ([47c5570](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/47c55701b4d333a88c38d7999870dfca189169ea))
* warn or refuse when a copied job runs a deployed child job ([#95](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/95)) ([535c7bf](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/535c7bf3b4e6ef05199306b29f076fac72ba3f65))


### Bug Fixes

* add compute knobs for ephemeral jobs and fix two serverless gaps in sanitise ([#83](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/83)) ([6ccd411](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/6ccd411db4bf258c5f2e0b5e9039822aa1ca5fb2))
* read disabled tasks from the job copy and restore the sdk 0.66 floor ([#85](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/85)) ([37942ea](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/37942eaa410b70eba1c45cd278f33fa34adb2930))
* read resolved task parameters from each task run ([#115](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/115)) ([e454e89](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/e454e8965a3278f9f976444658b8bd0dc773630a))
* report lakeflow coordinates on every phase and summarise kept jobs ([#77](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/77)) ([30a8beb](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/30a8beb47de3056aafa9f9934039e37eb336f659))


### Documentation

* record the code style and test conventions from the simplification pass ([#114](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/114)) ([f6dda1f](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/f6dda1fc4130cbbb90087f41fcac59fba281b5bd))
* use Lakeflow variables in the per-branch deployment recipe ([#92](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/92)) ([2001268](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/20012687e75fe4a9bcd31ef110c4050bca1c3be1))

## [0.2.1](https://github.com/stevenayers/pytest-databricks-lakeflow/compare/v0.2.0...v0.2.1) (2026-10-18)


### Bug Fixes

* release automatically after the release PR merge ([#4](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/4)) ([0d2357f](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/0d2357fafb13ffa48f355d05dff571fcf584c8a4))
* run the release automation with a GitHub App token ([#6](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/6)) ([a257b01](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/a257b019ec4c956d2101035d42ddd8f2c8541eb2))

## [0.2.0](https://github.com/stevenayers/pytest-databricks-lakeflow/compare/v0.1.0...v0.2.0) (2026-10-18)


### ⚠ BREAKING CHANGES

* rename task value arguments and attributes ([#2](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/2))

### Features

* rename task value arguments and attributes ([#2](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/2)) ([224d531](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/224d5315e24af0735660d10a0a14e10599ff2cb9))

## [0.1.0](https://github.com/stevenayers/pytest-databricks-lakeflow/tree/v0.1.0) (2026-10-17)

### Features

* First release: job and task tests for Lakeflow Jobs defined in a Databricks Asset Bundle, with temporary schemas, task value stubs and probes, keep and sweep rules, and pytest-xdist support.
