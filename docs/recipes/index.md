# Recipes

Find the thing you want to do, then follow the link. Each recipe is a code block and the handful of rules you will hit. The pages group them: [Change the job settings](job-settings.md), [Control the run](runs.md), [Task types](task-types.md), [Test data](data.md), [Debug a failure](debugging.md), [Compute and cost](compute.md), [Bundle deployments](deployments.md) and [CI and housekeeping](ci.md).

## I want to...

### Write a test

| Goal | Recipe |
| --- | --- |
| Run a whole job and assert on the result | [Make a copy of a job](../job-tests.md#make-a-copy-of-a-job) |
| Run one task with the upstream values I choose | [Run one task on its own](../task-tests.md#run-one-task-on-its-own) |
| Give a task the SQL rows or alert state another task would have produced | [Stub a task's output](../task-tests.md#stub-a-tasks-output) |
| Read the task values a task set | [Read task values](../job-tests.md#read-task-values) |
| Re-run one task against the real values of an earlier run | [Replay a task from a previous run](../task-tests.md#replay-a-task-from-a-previous-run) |
| Run the downstream half of a job | [Run the downstream half of a job](runs.md#run-the-downstream-half-of-a-job), [Replay a run's task values](runs.md#replay-a-runs-task-values) |
| Run only some tasks | [Run only some tasks](../job-tests.md#run-only-some-tasks) |
| Run the same job twice and compare | [Run it](../job-tests.md#run-it) |
| Assert on a failure, its root cause or a Spark error class | [Assert on a failure](../job-tests.md#assert-on-a-failure) |
| Test an if/else condition on its own | [Test a condition task on its own](../task-tests.md#test-a-condition-task-on-its-own) |
| Run the body of a for-each task once, with one item | [Run a for-each body once](../task-tests.md#run-a-for-each-body-once) |
| Share one expensive run between several tests | [Share one run between tests](runs.md#share-one-run-between-tests) |
| Run tests in parallel | [Parametrise and run in parallel](runs.md#parametrise-and-run-in-parallel) |

### Change the job settings

| Goal | Recipe |
| --- | --- |
| Switch off a task that notifies people or refreshes a downstream system | [Disable a task](job-settings.md#disable-a-task) |
| Change the parameters of one task only | [Override one task's parameters](job-settings.md#override-one-tasks-parameters) |
| Test what happens when a task is retried or times out | [Test retries and timeouts](job-settings.md#test-retries-and-timeouts) |
| Run the copy as the deployed job's principal | [Run as another identity](job-settings.md#run-as-another-identity) |
| Install a package only the tests need | [Add test-only dependencies](job-settings.md#add-test-only-dependencies) |
| Install my own wheel's test extras | [Install a local wheel with extras](job-settings.md#install-a-local-wheel-with-extras) |
| Change a serverless environment's version or dependencies | [Change a serverless environment](job-settings.md#change-a-serverless-environment) |
| Change anything else in the job settings | [Change anything else before the job is created](job-settings.md#change-anything-else-before-the-job-is-created) |
| Set one option for every test in a directory | [Set a default for every copy](job-settings.md#set-a-default-for-every-copy) |
| Let job code detect that it runs under test | [Let job code know it is under test](data.md#let-job-code-know-it-is-under-test) |

### Control the run

| Goal | Recipe |
| --- | --- |
| Run several jobs at the same time in one test | [Run jobs side by side](runs.md#run-jobs-side-by-side) |
| Stop a streaming job after a fixed time and still get its result | [Stop a run that does not end](runs.md#stop-a-run-that-does-not-end) |
| Stay under a serverless quota | [Cap the number of active runs](runs.md#cap-the-number-of-active-runs) |
| Make the whole suite faster | [Speed and cost](../speed.md) |
| Choose fast start-up or fewer DBUs | [Choose the performance mode](compute.md#choose-the-performance-mode) |

### Task types

| Goal | Recipe |
| --- | --- |
| Read the rows a SQL file task returned | [Read the rows of a SQL file task](task-types.md#read-the-rows-of-a-sql-file-task) |
| Read the rows of a saved query task | [Read the rows of a saved query](task-types.md#read-the-rows-of-a-saved-query) |
| Check whether an alert triggered | [Alert tasks](task-types.md#alert-tasks) |
| Check which branch an if/else took | [Branches](task-types.md#branches) |
| Test a branch without arranging the data that selects it | [Force a branch](task-types.md#force-a-branch) |
| Run a for-each task over a short list and read each iteration | [For-each tasks](task-types.md#for-each-tasks) |
| Test what a job does when a file arrival or table update trigger fires | [Simulate a trigger](task-types.md#simulate-a-trigger) |
| Full-refresh or select tables in a pipeline task | [Choose how a pipeline task refreshes](task-types.md#choose-how-a-pipeline-task-refreshes) |
| Read the update a pipeline task started and its error events | [Read the pipeline update](task-types.md#read-the-pipeline-update) |
| Run a pipeline task against a copy that writes to my test schema | [Copy the pipelines of a job](task-types.md#copy-the-pipelines-of-a-job) |
| Test a pipeline on its own | [Copy a pipeline on its own](task-types.md#copy-a-pipeline-on-its-own) |
| Test a job whose task runs another job | [Jobs that run other jobs](task-types.md#jobs-that-run-other-jobs) |
| Run a dbt selection and read `run_results.json` | [dbt tasks](task-types.md#dbt-tasks) |

### Test data

| Goal | Recipe |
| --- | --- |
| Give each test its own schema | [Give each test its own schema](data.md#give-each-test-its-own-schema) |
| Create the fixtures every test reads, once per session | [Seed once for the whole session](data.md#seed-once-for-the-whole-session) |
| Seed tables or files before one test's job runs | [Seed data with a setup step](data.md#seed-data-with-a-setup-step) |
| Clean up files or tables outside the test schema | [Clean up with a teardown step](data.md#clean-up-with-a-teardown-step) |
| Assert on what the teardown removes | [Tear down when the job closes](data.md#tear-down-when-the-job-closes) |
| Seed with SQL or a notebook file instead of a function | [Seed with SQL statements or a notebook file](data.md#seed-with-sql-statements-or-a-notebook-file) |
| Run a setup or teardown step as a task of the job | [Run a step as a task](data.md#run-a-step-as-a-task) |
| Seed with pytester fixtures | [Seed from the test process](data.md#seed-from-the-test-process) |

### Debug a failure

| Goal | Recipe |
| --- | --- |
| See each task's state change while the run is in flight | [Watch a run live](debugging.md#watch-a-run-live) |
| Read what a failed notebook printed | [Read what a notebook printed](debugging.md#read-what-a-notebook-printed) |
| Keep the job to open it in the workspace | [Keep the job](debugging.md#keep-the-job) |
| Refine assertions against a kept run without running again | [Read a kept run again](debugging.md#read-a-kept-run-again) |
| Step through a failed notebook task on interactive compute | [Reproduce a failed notebook task](debugging.md#reproduce-a-failed-notebook-task) |
| Keep every run's output as a CI artefact | [Keep every run's output as an artefact](debugging.md#keep-every-runs-output-as-an-artefact) |

### Compute and cost

| Goal | Recipe |
| --- | --- |
| Skip cluster start-up with a running cluster | [Skip cluster start-up](compute.md#skip-cluster-start-up) |
| Run job clusters on one node | [Run job clusters on one node](compute.md#run-job-clusters-on-one-node) |
| Run without serverless jobs | [Run the helper tasks on a job cluster](compute.md#run-the-helper-tasks-on-a-job-cluster) |
| Pin the serverless environment version | [Pin the serverless environment version](compute.md#pin-the-serverless-environment-version) |
| Charge test runs to a budget policy | [Charge runs to a budget policy](compute.md#charge-runs-to-a-budget-policy) |
| Shrink clusters or queue runs | [Change compute with modify_settings](compute.md#change-compute-with-modify_settings) |
| Track run times over time | [Record run timings](compute.md#record-run-timings) |

### Deployments, CI and housekeeping

| Goal | Recipe |
| --- | --- |
| Choose the target or pass bundle variables | [Choose the target and pass bundle variables](deployments.md#choose-the-target-and-pass-bundle-variables) |
| Test against a target I deploy myself | [Skip the deploy](deployments.md#skip-the-deploy) |
| Stop branches and pipelines from breaking each other's runs | [Give each branch and identity its own deployment](deployments.md#give-each-branch-and-identity-its-own-deployment) |
| Remove the test deployment at the end | [Destroy the test deployment](deployments.md#destroy-the-test-deployment) |
| Run the suite in GitHub Actions or Azure DevOps | [Run the tests in CI](ci.md#run-the-tests-in-ci) |
| Refuse in CI what a developer's run only warns about | [Be stricter in CI](ci.md#be-stricter-in-ci) |
| Let engineers open the jobs a service principal kept | [Let the team open kept jobs](ci.md#let-the-team-open-kept-jobs) |
| Lint every job's wiring without running it | [Check every job's wiring without running it](ci.md#check-every-jobs-wiring-without-running-it) |
| Delete what crashed sessions left behind | [Clean up leftovers](ci.md#clean-up-leftovers) |
| Sweep the workspace from a nightly script | [Sweep from a nightly script](ci.md#sweep-from-a-nightly-script) |
