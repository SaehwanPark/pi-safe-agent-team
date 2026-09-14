# Round 10 — release-readiness / production-behavior audit

I audited current `main` at **`51c92300cc1db97e9c8a615c5eda36b6c8e58a38`**. The R9 work is substantive, and the new broker/journal chaos test is now a real improvement rather than a coordinator-only simulation. All **7 current checks are green**, including Windows/macOS/Linux, Pi + LCM smoke, R8 longevity, and the R9 broker/journal chaos soak.

The good news is that I no longer see a need for another architectural rethink. The remaining problems are mostly **bounded-state semantics, production-scale cost, and release/upgrade engineering**. Because you want `v0.3.0` after this round, I would treat five items as pre-release gates.

| ID        | Severity                      | Finding                                                                                                |
| --------- | ----------------------------- | ------------------------------------------------------------------------------------------------------ |
| **R10-1** | **P1 release blocker**        | ACK tombstones fixed the finite replay horizon by creating an unbounded hot-state collection           |
| **R10-2** | **P1 release blocker**        | Pruning retired-resource tombstones permits resource-ID/version ABA reuse                              |
| **R10-3** | **P1 high**                   | `maxRetainedArtifacts` cannot recycle resolved entries once it reaches exactly the limit               |
| **R10-4** | **P1/P2 release gate**        | Production full-state rollback-copy cost under default 24h hot history is still unmeasured             |
| **R10-5** | **P1 release/migration gate** | `v0.3.0` needs an explicit persisted-state v1→v2 migration and real `v0.2.3` fixture test              |
| **R10-6** | **P2 release packaging**      | Published package would advertise benchmark/smoke scripts whose files are omitted from the npm tarball |
| **R10-7** | **P2 verification**           | Release workflow can publish without the chaos/LCM gates; chaos itself is Ubuntu-only                  |
| **R10-8** | **P3 release polish**         | README/version/npm messaging and CHANGELOG need a public-facing 0.3.0 pass                             |

## R10-1 — ACK proofs are now correct but unbounded on the hot transaction path

R9 correctly fixed the dangerous case where an ACK succeeds, its message gets pruned, its generic idempotency entry ages out, and the retry can no longer prove success.

The new `acknowledgedMessages` map preserves exact `(recipient, message ID, revision)` proofs. That part is correct.

The problem is that I can find **no pruning or externalization path for these tombstones**. Every successful ACK adds another entry, and `exportState()` includes the complete collection.

Production broker mutations still take a rollback snapshot with `exportState()` before the operation.

So an all-day fabric trends toward:

```text
every acknowledged message
    ↓
permanent ACK tombstone
    ↓
copied on every broker mutation
    ↓
persisted in checkpoints
```

This recreates R7/R8's historical-state problem in a new collection.

### Recommended fix

Do **not** solve this by putting ACK proofs back behind another small LRU; that recreates R9-1.

Move ACK proofs to a genuinely cold durability structure, analogous in spirit to retained artifacts but optimized for high write volume:

```text
hot coordinator
  └── recent messages only

cold ACK proof store
  └── exact (recipient, messageId, revision) proofs
```

I would prefer an append-oriented compact store rather than rewriting one JSON manifest on every ACK. It can periodically compact itself.

The important invariant is:

> ACK retry correctness may outlive hot coordinator history, but ACK proof history must not participate in every coordinator transaction.

**Release blocker.**

---

## R10-2 — resource tombstone pruning creates an ABA bug

R9 now moves retired resources into compact tombstones and bounds those tombstones with `maxArchivedRecords`. That's directionally right.

But resource creation only rejects reuse while an archived tombstone with that ID still exists:

```text
resource X retired
→ tombstone X exists
→ redefining X rejected

enough later resources retire
→ tombstone X pruned

resource X defined again
→ accepted
→ version starts at 1
```

Meanwhile the resource snapshot token remains simply:

```text
resourceId@version
```

That produces classic ABA:

```text
old incarnation: parser@1
retire + tombstone eventually pruned

new incarnation: parser@1

old token == new token
```

A consumer holding an old task/handoff snapshot can no longer distinguish them.

The current R9 test checks that one of the **surviving** tombstones prevents reuse, but doesn't attempt to reuse one of the resource IDs whose tombstone was pruned.

### Recommended fix

Give every resource a stable incarnation/generation:

```text
resourceId
incarnation = UUID/random durable identity
version = mutable-content generation
```

Snapshot identity becomes conceptually:

```text
(resourceId, incarnation, version)
```

The human-facing token could still be compact/opaque.

Then resource names can safely be reused after historical pruning without confusing old references.

**Release blocker.**

---

## R10-3 — resolved retained artifacts cannot free capacity at the limit

The cold artifact design itself is much better in R9. The broker now stores the metadata separately in `retained-artifacts.json` with atomic replacement rather than copying the full records through ordinary broker transactions.

There is, however, a small boundary-condition bug.

Before adding a new artifact:

```ts
trimRetainedArtifacts()
assert(size < maxRetainedArtifacts)
```

But the trim method immediately returns while:

```text
size <= limit
```

So at exactly the configured limit:

```text
4096 records
some are resolved
→ trim returns without pruning
→ new retained artifact rejected
```

Because insertion is rejected before size can ever become `limit + 1`, resolved entries no longer make space after the cap has been reached.

The R9 test verifies that unresolved artifacts hit the configured limit, but does not test:

```text
fill limit
→ resolve one
→ retain another
```

Fix the insertion-aware trim to prune resolved entries while `size >= limit` when admission needs one slot.

This matters operationally because artifact-marking failures are deliberately handled conservatively; failure to classify a newly retained worktree can keep its terminal agent hot.

---

## R10-4 — we still haven't measured the actual production scaling path

We now have two useful benchmarks, but they deliberately test opposite halves of the problem.

R8 keeps a large default-retention hot history, but explicitly **reuses one rollback snapshot** to avoid measuring the production snapshot-copy cost.

R9 uses real `BrokerServer`/`BrokerClient`/journal/restarts, which is excellent, but its chaos configuration deliberately keeps history tiny:

```text
messageRetention = 8
historyRetentionMs = 10 ms
maxArchivedRecords = 32
```

Production still does:

```text
every mutating request
→ coordinator.exportState(...)
→ mutate
→ journal
```

So what is missing is:

> **real broker + normal 24-hour hot history**

This should become `bench:r10-production-soak`.

I would populate perhaps 2k–5k realistically sized hot tasks/messages/resources using default retention, then perform broker-level heartbeats, message sends/ACKs, task transitions, resource operations, status snapshots, and checkpoints while measuring p50/p95/p99 latency, RSS/heap, rollback-image size, journal/checkpoint size, and event-loop stalls.

A particularly worthwhile optimization after measurement is heartbeat: an agent heartbeat without leases currently does not need a durable event, but because it is still classified as a mutation the broker pays for the whole rollback snapshot first. That can become expensive with many live workers and a large hot history.

I would not insist that you completely replace rollback snapshots before `v0.3.0` **if the production-scale benchmark demonstrates acceptable bounds**. But right now that evidence is missing.

---

# R10-5 — `v0.3.0` should introduce persisted-state schema version 2

This is specifically important because we're preparing a public release.

Public `v0.2.3` persists:

```text
version: 1
agents/tasks/resources/messages/requests
dedupe
optional broker/resource sequences
optional idempotency
```

Current main still says:

```text
version: 1
```

but that same schema now additionally has model-turn waiters, archived histories, external retained-artifact references, ACK proofs, provider recovery reservations, and archived resources.

Forward loading of an old v0.2.3 checkpoint appears reasonably defensive by inspection because many new fields are optional.

The larger problem is **downgrade safety**.

If v0.3 writes its substantially richer state but still labels it version 1, an old v0.2.3 installation can plausibly accept that file as “version 1” and simply ignore semantics it doesn't understand.

That is worse than a clean rejection.

### For 0.3.0 I recommend

Make the **persistent coordinator state version 2**, while keeping the wire protocol version 1 if no incompatible wire change is needed.

Then:

```text
v0.3:
  read state v1
  explicitly migrate → v2
  write only v2

v0.2.3:
  sees v2
  rejects instead of silently downgrading
```

Also check in an **actual v0.2.3 fixture**, generated by the tagged `4cc71fa...` implementation rather than hand-written current types. It should include at least an active root/child, tasks, messages/request, a resource/lease, and a checkpoint plus journal tail. Boot current code against it and verify recovery/migration/reconnect.

Current journal replay directly feeds checkpoint state into the coordinator; this is the right seam for a formal migration layer.

For the public release, document state-directory upgrade as one-way, or create a backup before first v2 checkpoint.

I would treat this as a **v0.3 release blocker even though it isn't a current-main runtime bug**.

---

# Release-specific items

There are a few things I would deliberately fix only after the R10 runtime changes stabilize.

### Packaging

`package.json` exposes scripts such as:

```text
smoke:pi
smoke:pi:with-lcm
bench:r7
bench:r8
bench:r9-chaos
```

but the `files` whitelist includes neither `scripts/` nor `bench/`.

So the npm package can publish commands whose implementation files don't exist in the tarball.

For 0.3.0 either include those directories or remove package scripts that aren't intended for installed-package use. Since the README publicly advertises the benchmarks, I'd lean toward including `bench/`; smoke-development scripts could reasonably remain source-only if their npm scripts are also removed from the published surface.

Add a real tarball smoke:

```text
npm pack
→ unpack/install tgz in clean temp directory
→ load extension
→ run minimal broker test
```

`npm pack --dry-run` alone is weaker. The current release workflow only runs check/build/dry-run before publication.

### Release verification

The current R9 chaos CI is genuinely useful and green.

But the **release** workflow does not run R8/R9 or the Pi+LCM smoke before publishing. I would either make release depend on the already-green commit CI or rerun the critical release suite before `npm publish`.

I'd also run at least a short R9 chaos variant on Windows and macOS before 0.3.0. Ordinary tests already cover both platforms, but the actual chaos job is Ubuntu-only, while IPC and filesystem durability boundaries differ.

### Public-facing docs

The README still has several pre-release leftovers:

* version badge says `0.2.3`;
* npm publication says “Deferred” despite an active npm-publish workflow;
* comparison column still says `(v0.1)`;
* one operational-boundary section still describes “v0.1”.

The current CHANGELOG is excellent as an engineering history, but the `Unreleased` section is organized largely as “Round 4/5/6/7/8/9 hardening.”

For the public `0.3.0` release I'd preserve those audit details underneath, but lead with user-visible themes:

* crash/reconnect convergence;
* durable messaging and exact ACK semantics;
* local-model capacity/fairness;
* context/LCM resilience;
* safe retained Git artifacts;
* bounded long-running state;
* observability and operator commands;
* broker chaos/recovery verification.

That will communicate the scale of the 54 commits since v0.2.3 much better than exposing the internal audit chronology.

---

# R9 fixes I would consider successfully closed

Several important things from Round 9 look solid enough that I would **not reopen them**:

The worktree/session retention paths are now independent, so preserving useful committed Git work no longer automatically keeps the large child Pi transcript.

Physical provider capacity now survives an independent broker restart through a recovery reservation, and reconnecting hosts explicitly reconcile whether the old provider call is still running.

The model-turn grant liveness bug is addressed by giving a missed grant one compatibility retry, then moving the nonclaiming ticket to the FIFO tail.

And `/agents` now reports authoritative totals plus explicit truncation rather than silently pretending its bounded page is complete.

Those are meaningful improvements.

# Recommended implementation order

1. **R10-1:** externalize/bound ACK proof storage without restoring a finite replay horizon.
2. **R10-2:** introduce resource incarnation identity and make snapshot tokens generation-safe.
3. **R10-3:** fix retained-artifact admission at the exact capacity limit.
4. **R10-5:** introduce persisted state v2 + real v0.2.3 fixture migration and downgrade rejection.
5. **R10-4:** add production-default broker soak and optimize rollback copying only where measurements say it's necessary.
6. Then do the `v0.3.0` packaging/release pass: tarball smoke, cross-platform release tests, package version, README, CHANGELOG, release notes, npm publishing decision.

## Round 10 verdict

I would **not tag `v0.3.0` yet**, but the project is very close to the right kind of release boundary.

The two remaining runtime correctness blockers are now narrow and well-defined:

> **durable ACK evidence must be long-lived without becoming hot unbounded state**, and
> **resource identity must survive historical pruning without ABA reuse**.

After those, the biggest requirement is not another conceptual audit—it is proving that the production broker stays responsive with a realistic 24-hour hot state and proving that an actual `v0.2.3` state directory upgrades safely.

Once you address R10, I suggest we do a **short dedicated v0.3.0 release-candidate audit**, not an R11 architecture round: migration fixture, production soak results, package contents, docs/changelog, version bump, release workflow, and final release notes.
