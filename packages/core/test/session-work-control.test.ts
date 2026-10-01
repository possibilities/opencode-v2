import { describe, expect } from "bun:test"
import { DateTime, Deferred, Effect, Fiber } from "effect"
import { TestClock } from "effect/testing"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { SessionInput } from "@opencode-ai/core/session/input"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { SessionSchema } from "@opencode-ai/core/session/schema"
import { SessionWorkControl } from "@opencode-ai/core/session/work-control"
import { testEffect } from "./lib/effect"

const it = testEffect(AppNodeBuilder.build(SessionWorkControl.node))
const sessionID = SessionSchema.ID.make("ses_work_control")
const work = (id: string): SessionInput.Work => ({ id, inputs: new Set(), pending: new Set() })
const admitted = SessionInput.Admitted.make({
  id: SessionMessage.ID.make("msg_guarded"),
  sessionID,
  prompt: { text: "Steer" },
  delivery: "steer",
  admittedSeq: 1,
  promotedSeq: 1,
  timeCreated: DateTime.makeUnsafe(0),
})

const hold = (current: SessionInput.Work) =>
  Effect.gen(function* () {
    const control = yield* SessionWorkControl.Service
    const started = yield* Deferred.make<void>()
    const gate = yield* Deferred.make<void>()
    const fiber = yield* control
      .run(sessionID, current, Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(gate))))
      .pipe(Effect.forkChild)
    yield* Deferred.await(started)
    return { fiber, gate }
  })

const waitForPending = (count: number) =>
  Effect.gen(function* () {
    const control = yield* SessionWorkControl.Service
    while ((yield* control.pending(sessionID)) !== count) yield* Effect.yieldNow
  })

describe("SessionWorkControl", () => {
  it.effect("interrupts the exact captured active owner", () =>
    Effect.gen(function* () {
      const control = yield* SessionWorkControl.Service
      const current = work("work_active_interrupt")
      const owner = yield* hold(current)
      yield* control.interrupt(sessionID, current.id)
      expect(yield* Fiber.await(owner.fiber)).toMatchObject({ _tag: "Failure" })
    }),
  )

  it.effect("rejects stale interrupt and steer without affecting the successor", () =>
    Effect.gen(function* () {
      const control = yield* SessionWorkControl.Service
      const first = yield* hold(work("work_a"))
      yield* Deferred.succeed(first.gate, undefined)
      yield* Fiber.join(first.fiber)
      const second = yield* hold(work("work_b"))
      expect(yield* control.interrupt(sessionID, "work_a").pipe(Effect.flip)).toMatchObject({
        _tag: "Session.WorkChangedError",
      })
      expect(
        yield* control
          .submit({
            id: SessionMessage.ID.create(),
            fingerprint: "fixture",
            sessionID,
            workID: "work_a",
            commit: () => Effect.die("must not admit"),
          })
          .pipe(Effect.flip),
      ).toMatchObject({ _tag: "Session.WorkChangedError" })
      yield* Deferred.succeed(second.gate, undefined)
      expect(yield* Fiber.await(second.fiber)).toMatchObject({ _tag: "Success" })
    }),
  )

  it.effect("removes a cancelled queued steer before a later safe boundary", () =>
    Effect.gen(function* () {
      const control = yield* SessionWorkControl.Service
      const current = work("work_cancel")
      const owner = yield* hold(current)
      let commits = 0
      const request = yield* control
        .submit({
          id: SessionMessage.ID.create(),
          fingerprint: "fixture",
          sessionID,
          workID: current.id,
          commit: () =>
            Effect.sync(() => {
              commits++
              return admitted
            }),
        })
        .pipe(Effect.forkChild)
      yield* waitForPending(1)
      yield* Fiber.interrupt(request)
      expect(yield* control.pending(sessionID)).toBe(0)
      expect(yield* control.drain(sessionID, current)).toBe(0)
      expect(commits).toBe(0)
      yield* Deferred.succeed(owner.gate, undefined)
      yield* Fiber.join(owner.fiber)
    }),
  )

  it.effect("expires queued steers without any later admission", () =>
    Effect.gen(function* () {
      const control = yield* SessionWorkControl.Service
      const current = work("work_timeout")
      const owner = yield* hold(current)
      const request = yield* control
        .submit({
          id: SessionMessage.ID.create(),
          fingerprint: "fixture",
          sessionID,
          workID: current.id,
          commit: () => Effect.die("must not admit"),
        })
        .pipe(Effect.forkChild)
      yield* waitForPending(1)
      yield* TestClock.adjust("30 seconds")
      expect(yield* Fiber.await(request)).toMatchObject({ _tag: "Failure" })
      expect(yield* control.pending(sessionID)).toBe(0)
      expect(yield* control.drain(sessionID, current)).toBe(0)
      yield* Deferred.succeed(owner.gate, undefined)
      yield* Fiber.join(owner.fiber)
    }),
  )

  it.effect("bounds pending requests and rejects all remaining requests on work termination", () =>
    Effect.gen(function* () {
      const control = yield* SessionWorkControl.Service
      const current = work("work_bound")
      const owner = yield* hold(current)
      const requests = yield* Effect.forEach(Array.from({ length: SessionWorkControl.MAX_PENDING_STEERS }), () =>
        control
          .submit({
            id: SessionMessage.ID.create(),
            fingerprint: "fixture",
            sessionID,
            workID: current.id,
            commit: () => Effect.die("must not admit"),
          })
          .pipe(Effect.forkChild),
      )
      yield* waitForPending(SessionWorkControl.MAX_PENDING_STEERS)
      expect(
        yield* control
          .submit({
            id: SessionMessage.ID.create(),
            fingerprint: "fixture",
            sessionID,
            workID: current.id,
            commit: () => Effect.die("must not admit"),
          })
          .pipe(Effect.flip),
      ).toMatchObject({ _tag: "Session.WorkChangedError" })
      yield* Deferred.succeed(owner.gate, undefined)
      yield* Fiber.join(owner.fiber)
      for (const request of requests) expect(yield* Fiber.await(request)).toMatchObject({ _tag: "Failure" })
      expect(yield* control.pending(sessionID)).toBe(0)
    }),
  )

  it.effect("rejects a new steer when the work membership cap is exhausted", () =>
    Effect.gen(function* () {
      const control = yield* SessionWorkControl.Service
      const current = work("work_full")
      for (let index = 0; index < SessionInput.MAX_WORK_INPUTS; index++)
        current.inputs.add(SessionMessage.ID.make(`msg_${index}`))
      const owner = yield* hold(current)
      expect(
        yield* control
          .submit({
            id: SessionMessage.ID.create(),
            fingerprint: "fixture",
            sessionID,
            workID: current.id,
            commit: () => Effect.die("must not admit"),
          })
          .pipe(Effect.flip),
      ).toMatchObject({ _tag: "Session.WorkChangedError" })
      yield* Deferred.succeed(owner.gate, undefined)
      yield* Fiber.join(owner.fiber)
    }),
  )
  it.effect("coalesces retries even with a full queue and rejects conflicting pending payloads", () =>
    Effect.gen(function* () {
      const control = yield* SessionWorkControl.Service
      const current = work("work_dedupe_full")
      const owner = yield* hold(current)
      let commits = 0
      const inputs = Array.from({ length: SessionWorkControl.MAX_PENDING_STEERS }, () => ({
        sessionID,
        workID: current.id,
        id: SessionMessage.ID.create(),
        fingerprint: "same",
        commit: () =>
          Effect.sync(() => {
            commits++
            return admitted
          }),
      }))
      const requests = yield* Effect.forEach(inputs, (input) => control.submit(input).pipe(Effect.forkChild))
      yield* waitForPending(SessionWorkControl.MAX_PENDING_STEERS)
      const duplicate = yield* control.submit(inputs[0]!).pipe(Effect.forkChild)
      while ((yield* control.waiting(sessionID)) !== SessionWorkControl.MAX_PENDING_STEERS + 1) yield* Effect.yieldNow
      expect(
        yield* control
          .submit({ ...inputs[0]!, fingerprint: "changed" })
          .pipe(Effect.catchDefect((error) => Effect.succeed(error))),
      ).toMatchObject({ _tag: "SessionInput.LifecycleConflict" })
      expect(yield* control.drain(sessionID, current)).toBe(SessionWorkControl.MAX_PENDING_STEERS)
      expect(yield* Fiber.join(duplicate)).toEqual(admitted)
      for (const request of requests) expect(yield* Fiber.join(request)).toEqual(admitted)
      expect(commits).toBe(SessionWorkControl.MAX_PENDING_STEERS)
      yield* Deferred.succeed(owner.gate, undefined)
      yield* Fiber.join(owner.fiber)
    }),
  )

  it.effect("one cancelled duplicate waiter does not withdraw another waiter's request", () =>
    Effect.gen(function* () {
      const control = yield* SessionWorkControl.Service
      const current = work("work_waiters")
      const owner = yield* hold(current)
      let commits = 0
      const input = {
        sessionID,
        workID: current.id,
        id: SessionMessage.ID.create(),
        fingerprint: "same",
        commit: () =>
          Effect.sync(() => {
            commits++
            return admitted
          }),
      }
      const first = yield* control.submit(input).pipe(Effect.forkChild)
      const second = yield* control.submit(input).pipe(Effect.forkChild)
      while ((yield* control.waiting(sessionID)) !== 2) yield* Effect.yieldNow
      yield* Fiber.interrupt(first)
      expect(yield* control.pending(sessionID)).toBe(1)
      expect(yield* control.drain(sessionID, current)).toBe(1)
      expect(yield* Fiber.join(second)).toEqual(admitted)
      expect(commits).toBe(1)
      yield* Deferred.succeed(owner.gate, undefined)
      yield* Fiber.join(owner.fiber)
    }),
  )

  it.effect("one shared deadline rejects all duplicate waiters and prevents late retry revival", () =>
    Effect.gen(function* () {
      const control = yield* SessionWorkControl.Service
      const current = work("work_shared_deadline")
      const owner = yield* hold(current)
      const input = {
        sessionID,
        workID: current.id,
        id: SessionMessage.ID.create(),
        fingerprint: "same",
        commit: () => Effect.die("must not admit"),
      }
      const first = yield* control.submit(input).pipe(Effect.forkChild)
      const second = yield* control.submit(input).pipe(Effect.forkChild)
      while ((yield* control.waiting(sessionID)) !== 2) yield* Effect.yieldNow
      yield* TestClock.adjust("30 seconds")
      for (const request of [first, second])
        expect(yield* Fiber.join(request).pipe(Effect.flip)).toMatchObject({ _tag: "Session.WorkChangedError" })
      expect(yield* control.submit(input).pipe(Effect.flip)).toMatchObject({ _tag: "Session.WorkChangedError" })
      expect(yield* control.drain(sessionID, current)).toBe(0)
      yield* Deferred.succeed(owner.gate, undefined)
      yield* Fiber.join(owner.fiber)
    }),
  )
})
