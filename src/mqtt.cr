# The module definitions live in `base.cr`, which requires nothing from this
# shard. That is deliberate: every other file requires `base` rather than this
# aggregator, so requiring any entry point directly — `mqtt/client`,
# `mqtt/v5/client`, `mqtt/v3/client` — cannot re-enter a half loaded module.
require "./mqtt/base"
require "./mqtt/v3/*"
require "./mqtt/client"
