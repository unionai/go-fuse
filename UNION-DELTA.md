# Union delta ledger

This fork's `union` branch is a **consumption branch**: it merges the
contribution branches below and is what Union tags releases from
(consumed by unionai/juicefs via a `go.mod` replace). Upstream never
sees `union`.

Branch layout:

- `master` — tracks juicedata/go-fuse master. Sync freely; never commit here.
- `union` — merge-only. Union-specific housekeeping commits (like this
  file) live here and nowhere else.
- Contribution branches (below) — based on upstream lines, pristine.
  Fix passthrough bugs THERE first, then merge forward into `union`
  (upstream-first). Never rebase them onto `union` or push union-only
  commits to them: they are live upstream PR heads.

## Deltas vs. the juicedata line we build on

| Delta | Branch | Upstream status |
|---|---|---|
| FUSE passthrough binding (`FUSE_PASSTHROUGH`/`FOPEN_PASSTHROUGH`) | `haytham/passthrough` | **juicedata/go-fuse#53** (open, review promised post-1.4) |
| Broker RPC delegation of backing registration (env-gated: `UVOL_BROKER_SOCKET`) | `haytham/passthrough-broker-rpc` | candidate — offer after #53 lands |

## Base-line note (read before syncing from juicedata)

`union` is based on the juicedata **dev line** that juicefs actually pins
(`v2.1.1-0.20210611132105-24a1dfe6b4f8` lineage), NOT their `master`.
The two diverged in June 2021 and juicedata has never merged them.
Their `master` carried a cherry-pick of upstream's `/dev/fd/N` mountpoint
support and then reverted it (juicedata/go-fuse#28, no stated reason —
the dev line has its own fd-handoff implementation, which we rely on for
broker adoption). Do not "sync" `union` from their `master`; sync from
whatever line juicefs's `go.mod` pseudo-version points at, and re-verify
fd handoff (harness broker-chain leg) after every sync.

## Releases

Tags `v2.1.2-union.N` are cut from `union`. Consumers pin them via:

```
replace github.com/hanwen/go-fuse/v2 => github.com/unionai/go-fuse/v2 v2.1.2-union.N
```
