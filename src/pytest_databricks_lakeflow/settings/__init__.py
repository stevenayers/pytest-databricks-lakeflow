"""Turn a deployed job's settings into the settings of a throwaway copy.

`template` strips and names the copy, `compute` decides where its tasks run, `tasks` edits the task graph, `params`,
`for_each` and `stubs` inject the parameters and tasks a test asks for, `lifecycle` adds the setup and teardown tasks,
`selection` checks what `only=` selects, `saved_queries` inlines saved query tasks, `references` knows the `{{...}}`
references the platform resolves, and `lint` finds the wiring mistakes `bundle validate` passes.
"""
