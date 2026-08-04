# P3.6 signature: overflow tagged by ORIGINATING GATEWAY (not device).
# The fabric supplies the gateway id on every verdict (alert["gw"]); the
# single-host shared-log prototype has no such field, so this predicate — and
# the cross-gateway property below — is inexpressible there.
overflow_gw(string, int)
