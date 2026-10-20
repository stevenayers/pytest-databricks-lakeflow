# Changelog

All notable changes to this project are recorded here. The file is maintained by
[release-please](https://github.com/googleapis/release-please) from conventional commit messages.

## [0.13.1](https://github.com/stevenayers/pytest-databricks-lakeflow/compare/v0.13.0...v0.13.1) (2026-10-20)


### Documentation

* add a dark mode to the docs site ([#213](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/213)) ([d5c3739](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/d5c3739c9fe9072f75375b50df36a0106f7bdfbb))

## [0.13.0](https://github.com/stevenayers/pytest-databricks-lakeflow/compare/v0.12.0...v0.13.0) (2026-10-20)


### ⚠ BREAKING CHANGES

* Everything after `params` on `job()` and `task()`, after `only` on `run()` and `start()`, and after `key` on `check()` is keyword-only. `task()` gains `name=`.
* `pytest_databricks_lakeflow.serialise` is gone: call `run.as_dict()`. `EphemeralJob.pipelines` is `pipeline_copies`, also as a constructor keyword.
* Tests that caught `LakeflowError` for a wrong argument must catch `ValueError`: a choice outside the listed values, a reserved `__pytest_` name, a setup or teardown step the plugin cannot ship, a `task_params` or `for_each_inputs` shape, a `task_overrides` field, a `force_conditions` outcome, a stub value or output, a `trigger` value, an `environments` override, a lint rule name, `stop_after` longer than its `timeout`, `replay` without `only`, and `upstream_from` or `replay` of another job. The `teardown_at` message reads `teardown_at must be one of run, close, got 'x'`.

### Features

* add a recipes section to the docs with an "I want to..." index ([de95d69](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/de95d69f32ed89943aa6399dcd3903d667f9bfad))
* raise ValueError for a wrong argument and LakeflowError for a bundle, job or workspace problem ([de95d69](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/de95d69f32ed89943aa6399dcd3903d667f9bfad))
* replace serialise(run) with JobRun.as_dict() and EphemeralJob.pipelines with pipeline_copies ([de95d69](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/de95d69f32ed89943aa6399dcd3903d667f9bfad))
* take the options of job(), task(), run(), start() and check() by keyword ([de95d69](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/de95d69f32ed89943aa6399dcd3903d667f9bfad))

## [0.12.0](https://github.com/stevenayers/pytest-databricks-lakeflow/compare/v0.11.0...v0.12.0) (2026-10-20)


### Features

* add session defaults on Lakeflow for per-call options ([#200](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/200)) ([bf20100](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/bf2010068e956f48e3bf9db797276c92c3692b9c))
* add wheel, library, dashed-key and trigger rules to Lakeflow.check ([#202](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/202)) ([4f6da3c](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/4f6da3caf806607d50205cceb2a817f2900cd1e4))
* close the sweep's notebook and schema gaps and add Lakeflow.sweep() ([#204](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/204)) ([0269c70](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/0269c7011e223fe56656561106207323f0757179))
* inline saved queries with saved_queries="inline" so sql_result holds their rows ([#206](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/206)) ([8174faf](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/8174faf5910cb042d9890e5774c4683b3ea74a38))
* parse dbt artefacts with dbt_artifacts=True and pass dbt_commands per run ([#205](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/205)) ([016c53b](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/016c53b0257ac71bfa474bb62483d1b696f87f79))
* warn once when a workspace overrides PERFORMANCE_OPTIMIZED ([#199](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/199)) ([26b6135](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/26b6135d9280d1ea3f2e70b83fa95024a45d9159))


### Bug Fixes

* settle the last for-each and sweep gaps of the backlog pass ([#207](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/207)) ([bd39cee](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/bd39cee37ecdad220573c922000feed39e7a324e))


### Documentation

* say exactly what the sweep leaves for a bundle set up later ([#208](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/208)) ([ca221f9](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/ca221f92445854fdef6a88ab6df6a32df98dfc82))

## [0.11.0](https://github.com/stevenayers/pytest-databricks-lakeflow/compare/v0.10.0...v0.11.0) (2026-10-20)


### Features

* cap active runs per process with --lakeflow-max-concurrent-runs ([#194](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/194)) ([8ea383a](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/8ea383a2563ae51f961f2e88a2a6e405af2afb08))
* collect one check per job template with --lakeflow-check ([#193](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/193)) ([264bfce](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/264bfce62421c92bea3c129144e30a895580ccbd))
* run a for-each body once with lakeflow.task("job.loop.body", input=...) ([#196](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/196)) ([312f56e](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/312f56ec08256d3124013e63224dd5994bcd46a8))
* surface for-each iterations and stats on TaskRun ([#195](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/195)) ([329ba50](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/329ba5002dccd4d4ff8d193ce768e8099216af01))


### Bug Fixes

* land the wave 7 and wave 8 review notes ([#197](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/197)) ([f429f87](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/f429f873253eecb85caa7be3ee65ab7c8bb60334))
* settle the wave 8 for-each gaps: nested task_params, serverless retries in loops and task values that iterations cannot set ([#198](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/198)) ([7764d6d](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/7764d6dae263f97d39be94871ef4281cd1d9a75d))

## [0.10.0](https://github.com/stevenayers/pytest-databricks-lakeflow/compare/v0.9.0...v0.10.0) (2026-10-20)


### Features

* add --lakeflow-live and run(stream=True) to print task progress to the terminal ([#185](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/185)) ([03b36be](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/03b36be6a991513f1d264c36693be6758b10645f))
* add Lakeflow.attach(run_id) to rebuild a JobRun from a kept job's run ([#186](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/186)) ([be675cf](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/be675cf28e04a9677f6eadee3c6db0c867a2e190))
* add teardown_at="close" to run the teardown task when the job closes ([fdf6825](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/fdf682581004f4b5b08d6589f3f0259643ade74d))
* refuse an empty dbt command list, keep dbt artefact headers out of report.json and document dbt tasks ([#188](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/188)) ([7cffc0d](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/7cffc0d0938241496d3bfc9d9f4bfa37e1c709d2))


### Bug Fixes

* land the wave 6 and wave 7 review notes ([#189](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/189)) ([ff08b27](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/ff08b27410c66328285835ab5997d090ebbd3870))
* refuse running a disabled task through only= ([#190](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/190)) ([725256a](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/725256a6415debcd384f228a38b7330913b49b0c))

## [0.9.0](https://github.com/stevenayers/pytest-databricks-lakeflow/compare/v0.8.0...v0.9.0) (2026-10-20)


### Features

* add Lakeflow.check() to lint job templates without running them ([#176](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/176)) ([e9fdff8](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/e9fdff8122b1f93cb57bf629e72297500d83f296))
* copy bundle pipelines into a test schema with lakeflow.pipeline() and pipelines="copy" ([#178](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/178)) ([be367ef](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/be367ef2c8c0f03013f8e9a885421e5ae29d108c))
* override for_each_task inputs on the ephemeral copy ([#177](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/177)) ([d7b2f8c](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/d7b2f8c6833825733011f9090d6a3c7506d81c2b))
* simulate trigger context with trigger= on job() and task() ([#179](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/179)) ([5a91a4f](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/5a91a4fda42b6481eb34f09cc7f7ff7ee168f4de))


### Bug Fixes

* land the wave 5 and wave 6 review notes ([#181](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/181)) ([84dee67](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/84dee67536d918c33eec1e9cf1d2c4d64a88e27e))
* settle the wave 6 bugs in pipeline copies, for-each inputs and trigger references ([#182](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/182)) ([be1688b](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/be1688b5606aa0bd880291339bf8c0277f4a4853))

## [0.8.0](https://github.com/stevenayers/pytest-databricks-lakeflow/compare/v0.7.0...v0.8.0) (2026-10-20)


### Features

* check permissions and run_as before deploying or creating a copy ([#163](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/163)) ([cd5fd42](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/cd5fd4229aac6341829bf02196faafce0b568697))
* expose the pipeline update of a pipeline task and cover a full refresh end to end ([#161](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/161)) ([a60402d](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/a60402dfcbb9077fd05ddf0f1fb2aa779f6c638f))
* stub {{tasks.&lt;key&gt;.output.*}} references with Stub(values=, outputs=) ([#162](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/162)) ([e76111e](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/e76111e01a4fb51c757f37267f5b805a5f2cc326))
* warn when extra_dependencies cannot reach notebook tasks ([#167](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/167)) ([ba5b580](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/ba5b5801838c162fbef3a87c012507dada403ec9))
* write a debug notebook that replays a failed notebook task's resolved parameters ([#164](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/164)) ([36cc166](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/36cc166b12aa5862ea1e488d7c0118b192c5474b))


### Bug Fixes

* file tasks downstream of a disabled task under disabled_tasks and report an all-disabled run honestly ([#165](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/165)) ([2e5e011](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/2e5e0118faba5c84bfa5f0c24788278a00a9e1ce))
* land the wave 4 and wave 5 review notes and run the integration suite at -n 8 ([#170](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/170)) ([6e75576](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/6e755767cf92f892b39986582f639e57f0bd6d2e))
* settle the wave 5 bugs in pipeline, alert and debug notebook reporting and check the host before deploying ([#171](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/171)) ([174d65e](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/174d65e6f3d691a6fdf77b1474ada50b2c129c81))
* spread the workspace client retries of xdist workers that race on the CLI token refresh ([#166](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/166)) ([dd2a14e](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/dd2a14e7efde9d19da3df1b611fe1c16837a467d))


### Documentation

* record that a successful saved query task reports no rows ([#169](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/169)) ([80cb6f3](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/80cb6f3bec8fb58839a664cc7702d288b4e87c1c))

## [0.7.0](https://github.com/stevenayers/pytest-databricks-lakeflow/compare/v0.6.0...v0.7.0) (2026-10-19)


### Features

* accept SQL statements or a notebook file for setup= and teardown= ([#153](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/153)) ([09eb4d5](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/09eb4d5510a8adfe1d6a9dc8299540b34f28f1f5))
* add run(stop_after=) so streaming runs return a cancelled JobRun ([#155](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/155)) ([3f5a238](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/3f5a238e6cb6bd578b9bc6d2647e1e2622224044))
* resolve local wheels with extras in extra_dependencies and warn on stale uploads ([#154](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/154)) ([90126b4](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/90126b483d5917df823aee672de516af11b29648))
* stub upstream tasks on job() and replay a run's task values into them ([#150](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/150)) ([3962ff3](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/3962ff3324356217ee086f6821f299d584dd0565))


### Bug Fixes

* land the wave 4 review notes and the wave 3 triage fixes ([#156](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/156)) ([3062987](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/3062987f0da28c99709c0336e1b15757f0263539))

## [0.6.0](https://github.com/stevenayers/pytest-databricks-lakeflow/compare/v0.5.0...v0.6.0) (2026-10-19)


### Features

* add --lakeflow-report-dir with a report.json per test, raw logs and notebooks ([#142](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/142)) ([6513ee2](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/6513ee2d3a41a14dbd7dbc4dc0ffec263f31d6ce))
* add task_overrides to job() and task() and lengthen the job timeout for long runs ([#145](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/145)) ([aa4f0fb](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/aa4f0fb3b8293b0732c9ee4356a2c0c2efa2b5ef))
* add task_params to job() and task(), warning on job-parameter collisions ([#141](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/141)) ([84bb2f2](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/84bb2f2b111baf214c7d47516a49bbb06c885514))
* widen --lakeflow-sweep to all creators, schemas, notebooks, a dry run, a report and a keep TTL ([#144](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/144)) ([9c172d7](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/9c172d73239b6bedebd5ab75efb73c8515bf3c5a))


### Bug Fixes

* land the wave 3 review notes and the small fixes from the wave 1 and 2 triage ([#147](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/147)) ([67727fe](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/67727fe133b1cab4113b88fdc873df3077fdd70d))

## [0.5.0](https://github.com/stevenayers/pytest-databricks-lakeflow/compare/v0.4.0...v0.5.0) (2026-10-19)


### Features

* add per-job serverless environment overrides to job() and task() ([#128](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/128)) ([5f9cd3f](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/5f9cd3fb5528899901261d8e8c4dfdcd772cb101))
* add start() and RunHandle, a poll interval and task states on timeout ([#130](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/130)) ([3b9ec27](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/3b9ec27439f1c9aa69bac27bb301e20be50f05ab))
* read the rows of a SQL file task into TaskRun.sql_result ([#131](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/131)) ([d152151](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/d1521512e3aae3145196d509d8cf5013b9ef71ad))
* replay a task test with the task values a previous run captured ([#129](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/129)) ([2a8974d](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/2a8974d241e685208235b175d2a8209571a588e6))


### Bug Fixes

* validate poll_interval and environment overrides, and land the wave 2 review notes ([#132](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/132)) ([9b54e36](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/9b54e36336929a2c623bc206126148e44bf736b3))

## [0.4.0](https://github.com/stevenayers/pytest-databricks-lakeflow/compare/v0.3.0...v0.4.0) (2026-10-19)


### Features

* add extra_dependencies and a warning for unversioned wheel artifacts ([#120](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/120)) ([70b3084](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/70b30846a9b0c8384a092dc2d1cc47e7c3df14bb))
* add setup and teardown functions that run on the job's compute ([#122](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/122)) ([f3cca22](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/f3cca22b84237d1a0175d04275432a291a007ddd))
* expose notebook cell outputs and printed text through runs/export ([#121](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/121)) ([522c6d5](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/522c6d5854d6a6f325253d2af2905c6c35d514d5))
* force an if/else branch in a job copy with force_conditions= ([#119](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/119)) ([328476d](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/328476d7665f3178eee165e555d6b78b49dc625f))


### Bug Fixes

* keep only the stdout stream in notebook stdout and tidy wave 1 review notes ([#123](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/123)) ([af2aa78](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/af2aa780da15000fb6b36d20a00b944497353d20))
* raise the databricks-sdk floor to 0.100 so alert tasks survive the copy ([#118](https://github.com/stevenayers/pytest-databricks-lakeflow/issues/118)) ([7f78c5c](https://github.com/stevenayers/pytest-databricks-lakeflow/commit/7f78c5cf60fcee3866befb233aca3314b5d4186e))

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
