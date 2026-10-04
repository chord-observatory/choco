# Agents

Instructions for coding agents working in this repository live in
[CLAUDE.md](CLAUDE.md): the project overview, the build and test commands,
the code layout, and the rules that must keep holding.  The reasoning
behind those rules, with measurements and history, is in `docs/design/`,
one file per subsystem; start with the links at the end of CLAUDE.md.

Keep CLAUDE.md to rules a future change would be wrong without; put the
why in `docs/design/`.  `choco.sh` must stay in step with any change to
install steps, dependencies or run commands, and `./choco.sh test` runs
the main suite plus the four job suites.
