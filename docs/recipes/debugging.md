# Debug a failure

What to reach for when a test fails and `assert run.succeeded, run` is not enough.

## Read the failure message

`assert run.succeeded, run` prints the run's repr: the failing tasks with the first line of their errors, the run's job parameters and a link to the run. `run.raise_for_failure()` adds each failing task's whole error, its root cause and its own run URL, see [Get the whole failure in one call](../job-tests.md#get-the-whole-failure-in-one-call). `task.error.root_cause` and `task.error.code` pick the real cause out of a Spark trace, see [Assert on a failure](../job-tests.md#assert-on-a-failure).

The same information is in the test's report. Each test's job and run URLs are appended to `item.user_properties`, so they appear in JUnit XML, and a `lakeflow` section on the test report carries the URLs, the task state lines and, for a failed run, the failing tasks and their resolved parameters. The [reference](../reference/output.md) lists every property and line.

## Watch a run live

```bash
pytest --lakeflow-live
```

prints the start of each run, each task's change of state and the end of the run straight to the terminal while the test is running, prefixed `lakeflow: `:

```text
lakeflow: Started run 77 of job 1001: https://<workspace host>/jobs/runs/77
lakeflow: transform: PENDING -> RUNNING
lakeflow: transform: RUNNING -> FAILED in 0:00:07 ValueError: no rows
lakeflow: Run 77 finished FAILED in 0:00:51: https://<workspace host>/jobs/runs/77
```

- `job.run(stream=True)` does the same for one run, and `stream=False` keeps one run quiet under `--lakeflow-live`.
- `pytest --log-cli-level=INFO` shows the same task lines plus the deploy, create and delete lines with their URLs, from every logger.
- Under pytest-xdist a worker cannot write to the terminal, so the lines are only logged and kept in the report section.

## Read what a notebook printed

The Jobs API returns no stdout for a notebook task, but it renders the notebook of a task run with every cell's output. After a run, the plugin exports the notebook of each notebook task that ran and did not succeed and parses it into `task.notebook`:

```python
def test_transform_prints_its_input(lakeflow: Lakeflow) -> None:
    with lakeflow.job("my_example_job", params={"param1": "forbidden", "param2": "world"}) as job:
        run = job.run()
        notebook = run.task("transform").notebook
        assert notebook is not None
        assert "transform: checking forbidden" in notebook.stdout
        assert notebook.failed_cell is not None
        assert notebook.failed_cell.error == "PermissionError: forbidden may not read /Volumes/example/raw/landing"
        notebook.save("build/lakeflow/transform.html")
```

- `notebook.stdout` is what every cell wrote to standard output, in order: what `print()` produced, without standard error, displayed results or the `exit()` value. `notebook.cells` holds each `NotebookCell` with `index`, `code`, `output` (printed or displayed text, HTML and colour codes removed), `output_html`, `error` (`Name: message`, or `None`) and `stdout`. `failed_cell` is the first cell that raised. `html` is the export unchanged, and `save(path)` writes it to a file that opens in a browser like the run page.
- `Lakeflow(notebook_export=...)` chooses the tasks: `"failed"` (the default) exports the notebook tasks that ran and did not succeed, `"always"` every notebook task that ran, so tests can assert on what a passing notebook printed, and `"never"` none. Only the last attempt of a retried task is exported, and never the plugin's own tasks.
- When a test fails, its `lakeflow` report section shows, for each failing task with a notebook, the first code line of the cell that failed and the last 50 lines of `stdout`.
- The export format is not documented, so the plugin parses it best effort: when the notebook model cannot be found, `cells` is empty, `html` keeps the export, and a `UserWarning` is emitted once per test. When the export fails, `notebook` is `None` and a warning is logged.

## Keep the job

- `keep_failed=True`, the default, keeps the job and its pipeline copies when the test fails. The job and run URLs are logged at WARNING, added to the test report, and listed in the `lakeflow` block at the end of the session.
- `pytest --lakeflow-keep` keeps every job, pipeline copy and schema of the run, and skips the teardown runs of `teardown_at="close"`.
- `pytest --no-lakeflow-keep-failed` deletes jobs even when their test fails, for CI.
- `pytest --lakeflow-keep-ttl=48h` (or `7d`) tags every job with `pytest_expires`, that far ahead in UTC. The sweep keeps a tagged job until then, however old it is, so `pytest --lakeflow-keep --lakeflow-keep-ttl=7d` keeps a run's jobs for a week of debugging.
- `pytest --lakeflow-sweep` deletes the jobs you kept earlier, once they are older than `sweep_hours`, see [Clean up leftovers](ci.md#clean-up-leftovers).

A kept job carries the test node id in its name and description, and the CI run URL, user, commit and branch in its tags, so it can be traced back from the Jobs list.

## Read a kept run again

`lakeflow.attach(run_id)` rebuilds the `JobRun` of a finished run of a kept job without running anything, so you can refine assertions against real output instead of running the job again. Pass the job run id, the number at the end of the `Kept run:` URL:

```python
def test_transform_again(lakeflow):
    run = lakeflow.attach(5437110661)
    assert run.job_key == "my_example_job"
    assert run.task("transform").raised(PermissionError)
```

- The job must still exist, which `keep_failed`, `keep` and `--lakeflow-keep` see to. Its `pytest_job_key` tag names the bundle job, and its settings say which tasks were stubs and which values the probes captured, so `tasks`, `injected_tasks` and `task_values` come back as `run()` returned them.
- `attach` deploys nothing, needs no bundle, and never cancels or deletes anything. It runs inside a test, which records the run in its report section, and does not call `run_finished`.
- It raises `LakeflowError` when the run does not exist, is a task run, has not finished, or belongs to a job that no longer exists or that the plugin did not create.

## Reproduce a failed notebook task

`job.debug_notebook("transform")` writes a notebook to your workspace that reproduces a notebook task of the job's last run: a first cell sets the task's resolved parameters as text widgets, and a second runs the task's notebook with `%run`. Open it, change a widget if you like, and run all cells to step through the failure on interactive compute:

```python
def test_transform(lakeflow):
    with lakeflow.job("my_example_job", params={"param1": "forbidden"}) as job:
        run = job.run()
        notebook = job.debug_notebook("transform")
        print(notebook.url)  # also in the test report as lakeflow_debug_notebook
        assert run.succeeded
```

- It returns a `DebugNotebook` with `path` and `url`. The notebook goes to `<helpers_path>/debug/<job key>__<task key>__<run id>`, replacing one written earlier for the same run, and stays until you delete it or `--lakeflow-sweep-notebooks` does. Pass `run=` to replay an earlier run of the same job.
- `pytest --lakeflow-debug-notebook` writes one for every notebook task that failed in the last run of each job of a failed test, when the job closes. Each URL goes to the report section and JUnit XML as `lakeflow_debug_notebook`, and the terminal summary lists them with their test and task. With `--lakeflow-report-dir`, `report.json` lists them under `debug_notebooks`.
- It raises `LakeflowError` for a task that is not a notebook task, a notebook that runs from Git, a job that has not run, or a task that did not run in that run.

The notebook replays the parameters, not the run:

- A notebook that reads upstream task values with `dbutils.jobs.taskValues.get()` raises `TypeError` outside a job unless it passes `debugValue=`.
- `%run` runs the notebook of the deployed test target, so a redeploy changes what it runs, and `--lakeflow-destroy` removes it.
- A text widget holds at most 2048 bytes. A longer value is written in full to a `parameters` dict in the widgets cell, and its widget holds a truncated copy, which is what the notebook reads. The terminal summary names such parameters, as `widgets cut at 2048 bytes: 'payload'`.
- In CI with a service principal the notebooks land in the service principal's home folder. Set `helpers_path` to a `/Shared/...` folder so engineers can open them.

## Keep every run's output as an artefact

```bash
pytest --lakeflow-report-dir=build/lakeflow
```

writes each test's runs to that directory, so a CI build can keep them after the jobs are deleted:

```text
build/lakeflow/
  test_jobs.py__test_job-3f9a2c1b/
    report.json            # the test's outcome and one entry per run
    77/load.log            # raw logs, by run id and task key
    77/transform.html      # an exported notebook, which opens in a browser
```

- `report.json` holds the test's outcome per phase, its debug notebooks, and one entry per run the test waited for, in the order they finished, plus one for each run the plugin cancelled: a run that hit its `timeout`, and a run from `start()` still active when its job closed. Each entry is `run.as_dict()` with the job URL, the job's `params` and the copy's settings.
- Logs and exported notebooks are written as files, named in the entry as `logs_file` and `notebook_file`. Pass `Lakeflow(notebook_export="always")` to keep every notebook, not only those of failed tasks.
- At the start of each session the plugin deletes the test directories an earlier session left in the directory, so a rerun never mixes stale output with its own.

The [reference](../reference/output.md#report-directory) describes the layout and every field of `report.json`.
