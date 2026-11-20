# CLAUDE.md
- The current project is the shuffle aggregation described in docs/shuffle/PROPOSAL.md. Read it before any change.
- This checkout is Elasticsearch **6.5.4** (tag v6.5.4, branch `shuffle-agg`), not 6.8 as the proposal assumes. docs/shuffle/code-map.md records every difference.
- Work one milestone at a time (section 9). Do not start a milestone until the previous one's acceptance criteria pass.
- Class and method names in the proposal were written from knowledge of ES 6.8 and MUST be verified against this checkout before editing. Record verified names and signatures in docs/shuffle/code-map.md.
- Keep changes minimal and local. Do not refactor unrelated code.
- Run `./gradlew precommit` and the tests named in the milestone before declaring it done.
- Build needs JAVA_HOME = JDK 11 (compiler) and runtime JDK 8 or 11. On this machine: `JAVA_HOME=~/.jdks/temurin-11`.
