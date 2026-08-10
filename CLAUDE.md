## Working on this project

This is an MQTT client shard for crystal lang, supporting 3.1.1 and 5.0. Use `crystal tool format` and `./bin/ameba` to format and lint code.

### Running specs

Prefer **`./test`**. It formats, lints and runs the whole suite, including the end to end specs that need a real broker, and it is what CI runs — so a green run locally means a green run there. Run specs using a subagent.

```bash
./test                          # everything, broker and all
./test spec/v5_live_spec.cr     # a single file
MQTT_LIVE_BROKER=host ./test    # against a broker you already have
```

`./test` finds a broker in this order: `MQTT_LIVE_BROKER` if set, a local `mosquitto` binary, then `docker compose` (see `docker-compose.yml`). `MQTT_LIVE_PORT` overrides the port, which defaults to 1883.

`crystal spec -v --error-trace` still works and is fine for the fake-broker specs. Without a broker the end to end specs report as **pending**, they do not fail. Watch for that: a pending count above zero means the broker backed specs did not run. You can also run individual files or use `focus: true` to isolate a spec.

Also run `CRYSTAL_WORKERS=4 crystal spec -Dpreview_mt` before finishing anything touching the client, transports or locking. CI has a job for it and the client is driven from several fibers.

### Spec layout

- `spec/v3_*.cr`, `spec/v5_*.cr` — deterministic specs against in-memory fakes
- `spec/*_live_spec.cr` — end to end against a real broker, gated behind `MQTT_LIVE_BROKER`
- `spec/support/fake_transport.cr` — in-memory transport, no sockets
- `spec/support/fake_broker.cr` — scriptable 3.1.1 broker, models retained storage
- `spec/support/fake_v5_broker.cr` — scriptable 5.0 broker, for enhanced auth and reason code failure paths
- `spec/support/fake_negotiating_broker.cr` — answers per the version the CONNECT asked for
- `spec/mosquitto.conf` — shared by `./test` and CI, pins the limits the 5.0 specs assert on

**Prefer an end to end spec against mosquitto** where the behaviour is the broker's: retained messages, retain handling, no local, subscription identifiers, session resumption. Reach for a fake only where a real broker cannot go — enhanced authentication needs a mosquitto plugin, a 3.1.1 only broker cannot be conjured from one that speaks 5.0, and reason code failure paths are not producible on demand.

Never use `next` to skip an example when a broker lacks a capability. It exits the example early and it **passes having asserted nothing**. Assert the capability instead, so an unsupported broker fails loudly.

## 1. Plan Node Default
- Enter plan mode for ANY non-trivial task (3+ steps or architectural decisions)
- If something goes sideways, STOP and re-plan immediately, don’t keep pushing
- Use plan mode for verification steps, not just building
- Write detailed specs upfront to reduce ambiguity

## 2. Subagent Strategy
- Use subagents liberally to keep main context window clean
- Offload research, exploration, and parallel analysis to subagents
- For complex problems, throw more compute at it via subagents
- One task per subagent for focused execution

## 3. Self-Improvement Loop
- After ANY correction from the user, update `tasks/lessons.md` with the pattern
- Write rules for yourself that prevent the same mistake
- Ruthlessly iterate on these lessons until mistake rate drops
- Review lessons at session start for relevant project

## 4. Verification Before Done
- Never mark a task complete without proving it works
- Diff behavior between main and your changes when relevant
- Ask yourself: "Would a staff engineer approve this?"
- Run tests, check logs, demonstrate correctness

## 5. Demand Elegance (Balanced)
- For non-trivial changes, pause and ask: "Is there a more elegant way?"
- If a fix feels hacky: "Knowing everything I know now, implement the elegant solution"
- Skip this for simple, obvious fixes, don’t over-engineer
- Challenge your own work before presenting it

## 6. Autonomous Bug Fixing
- When given a bug report, don’t ask for hand-holding
- Don’t start by trying to fix it. Instead, start by writing a test that reproduces the bug. Then, have subagents try to fix the bug and prove it by passing that test.
- Point at logs, errors, failing tests, then resolve them
- Zero context switching required from the user

---

## Task Management

1. **Plan First**: Write plan to `tasks/todo.md` with checkable items
2. **Verify Plan**: Check in before starting implementation
3. **Track Progress**: Mark items complete as you go
4. **Explain Changes**: High-level summary at each step
5. **Document Results**: Add review section to `tasks/todo.md`
6. **Capture Lessons**: Update `tasks/lessons.md` after corrections

---

## Core Principles

- **Simplicity First**: Make every change as simple as possible. Impact minimal code.
- **No Laziness**: Find root causes. No temporary fixes. Senior developer standards.

