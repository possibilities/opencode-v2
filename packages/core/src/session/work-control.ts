export * as SessionWorkControl from "./work-control"

import { Clock, Context, Deferred, Effect, Fiber, Layer, Schema } from "effect"
import { makeGlobalNode } from "../effect/app-node"
import { SessionInput } from "./input"
import { SessionSchema } from "./schema"
import { SessionMessage } from "./message"

export const MAX_PENDING_STEERS = 64
export const STEER_TIMEOUT_MS = 30_000
export const MAX_RETAINED_STEERS = 1024

export class ChangedError extends Schema.TaggedErrorClass<ChangedError>()("Session.WorkChangedError", {
  sessionID: SessionSchema.ID,
  workID: Schema.String,
}) {}

type Pending = {
  readonly id: SessionMessage.ID
  readonly fingerprint: string
  readonly deadline: number
  waiters: number
  readonly deferred: Deferred.Deferred<SessionInput.Admitted, ChangedError>
  readonly commit: () => Effect.Effect<SessionInput.Admitted, ChangedError>
  state: "queued" | "committing" | "done" | "cancelled"
}

type Entry = {
  readonly work: SessionInput.Work
  readonly fiber: Fiber.Fiber<unknown, unknown>
  readonly done: Deferred.Deferred<void>
  readonly pending: Pending[]
  readonly requests: Map<SessionMessage.ID, Pending>
  closing: boolean
}

export interface Interface {
  readonly run: <A, E, R>(
    sessionID: SessionSchema.ID,
    work: SessionInput.Work,
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E, R>
  readonly submit: (input: {
    readonly sessionID: SessionSchema.ID
    readonly workID: string
    readonly id: SessionMessage.ID
    readonly fingerprint: string
    readonly commit: (work: SessionInput.Work) => Effect.Effect<SessionInput.Admitted, ChangedError>
  }) => Effect.Effect<SessionInput.Admitted, ChangedError>
  readonly drain: (sessionID: SessionSchema.ID, work: SessionInput.Work) => Effect.Effect<number>
  readonly continue: (sessionID: SessionSchema.ID, work: SessionInput.Work, needed: boolean) => Effect.Effect<boolean>
  readonly close: (sessionID: SessionSchema.ID, work: SessionInput.Work) => Effect.Effect<void>
  readonly interrupt: (sessionID: SessionSchema.ID, workID: string) => Effect.Effect<void, ChangedError>
  readonly pending: (sessionID: SessionSchema.ID) => Effect.Effect<number>
  readonly waiting: (sessionID: SessionSchema.ID) => Effect.Effect<number>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/SessionWorkControl") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const active = new Map<SessionSchema.ID, Entry>()
    const reject = (sessionID: SessionSchema.ID, entry: Entry) => {
      entry.closing = true
      for (const pending of entry.pending.splice(0)) {
        if (pending.state !== "queued") continue
        pending.state = "cancelled"
        Deferred.doneUnsafe(pending.deferred, Effect.fail(new ChangedError({ sessionID, workID: entry.work.id })))
      }
    }

    return Service.of({
      run: (sessionID, work, effect) =>
        Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            const ready = yield* Deferred.make<void>()
            const done = yield* Deferred.make<void>()
            const fiber = yield* Deferred.await(ready).pipe(
              Effect.andThen(effect),
              Effect.onExit(() => Deferred.succeed(done, undefined)),
              Effect.interruptible,
              Effect.forkChild,
            )
            const entry: Entry = { work, fiber, done, pending: [], requests: new Map(), closing: false }
            if (active.has(sessionID)) return yield* Effect.die("Session work already has an owner")
            active.set(sessionID, entry)
            yield* Deferred.succeed(ready, undefined)
            return yield* restore(Fiber.join(fiber)).pipe(
              Effect.ensuring(
                Effect.sync(() => {
                  reject(sessionID, entry)
                  if (active.get(sessionID) === entry) active.delete(sessionID)
                }),
              ),
            )
          }),
        ),
      submit: (input) =>
        Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            const now = yield* Clock.currentTimeMillis
            const entry = active.get(input.sessionID)
            if (!entry || entry.work.id !== input.workID)
              return yield* new ChangedError({ sessionID: input.sessionID, workID: input.workID })
            const existing = entry.requests.get(input.id)
            if (existing && existing.fingerprint !== input.fingerprint)
              return yield* Effect.die(new SessionInput.LifecycleConflict({ id: input.id }))
            if (
              !existing &&
              (entry.closing ||
                entry.pending.length >= MAX_PENDING_STEERS ||
                entry.requests.size >= MAX_RETAINED_STEERS ||
                entry.work.inputs.size >= SessionInput.MAX_WORK_INPUTS)
            )
              return yield* new ChangedError({ sessionID: input.sessionID, workID: input.workID })
            const pending: Pending = existing ?? {
              id: input.id,
              fingerprint: input.fingerprint,
              deadline: now + STEER_TIMEOUT_MS,
              waiters: 0,
              deferred: Deferred.makeUnsafe(),
              commit: () => input.commit(entry.work),
              state: "queued",
            }
            if (!existing) {
              entry.requests.set(input.id, pending)
              entry.pending.push(pending)
            }
            pending.waiters++
            const cancel = () => {
              if (pending.state !== "queued") return
              pending.state = "cancelled"
              const index = entry.pending.indexOf(pending)
              if (index !== -1) entry.pending.splice(index, 1)
              Deferred.doneUnsafe(
                pending.deferred,
                Effect.fail(new ChangedError({ sessionID: input.sessionID, workID: input.workID })),
              )
            }
            return yield* restore(
              Deferred.await(pending.deferred).pipe(
                Effect.timeoutOrElse({
                  duration: Math.max(0, pending.deadline - now),
                  orElse: () =>
                    Effect.suspend(() => {
                      // Once the atomic commit starts, its response may be uncertain; never report a false 409.
                      if (pending.state === "committing" || pending.state === "done")
                        return Deferred.await(pending.deferred)
                      cancel()
                      return new ChangedError({ sessionID: input.sessionID, workID: input.workID })
                    }),
                }),
              ),
            ).pipe(
              Effect.ensuring(
                Effect.sync(() => {
                  pending.waiters--
                  if (pending.waiters === 0) cancel()
                }),
              ),
            )
          }),
        ),
      drain: (sessionID, work) =>
        Effect.gen(function* () {
          let promoted = 0
          while (true) {
            const now = yield* Clock.currentTimeMillis
            const entry = active.get(sessionID)
            if (!entry || entry.work !== work || entry.closing) return promoted
            const pending = entry.pending.shift()
            if (!pending) return promoted
            if (pending.state !== "queued") continue
            if (now >= pending.deadline) {
              pending.state = "cancelled"
              Deferred.doneUnsafe(pending.deferred, Effect.fail(new ChangedError({ sessionID, workID: work.id })))
              continue
            }
            pending.state = "committing"
            const exit = yield* pending.commit().pipe(Effect.exit)
            pending.state = "done"
            yield* Deferred.done(pending.deferred, exit)
            if (exit._tag === "Success") promoted++
          }
        }).pipe(Effect.uninterruptible),
      continue: (sessionID, work, needed) =>
        Effect.sync(() => {
          const entry = active.get(sessionID)
          if (!entry || entry.work !== work || entry.closing) return false
          if (needed || entry.pending.length > 0) return true
          // Queue admission and this closing transition are synchronous, with no check/act yield.
          reject(sessionID, entry)
          return false
        }),
      close: (sessionID, work) =>
        Effect.sync(() => {
          const entry = active.get(sessionID)
          if (entry?.work === work) reject(sessionID, entry)
        }),
      interrupt: (sessionID, workID) =>
        Effect.withFiber((caller) => {
          const entry = active.get(sessionID)
          if (!entry || entry.closing || entry.work.id !== workID) return new ChangedError({ sessionID, workID })
          reject(sessionID, entry)
          // Signal exactly this owner before yielding. Wait on work cleanup, avoiding concurrent fiber joins.
          entry.fiber.interruptUnsafe(caller.id)
          return Deferred.await(entry.done)
        }),
      pending: (sessionID) => Effect.sync(() => active.get(sessionID)?.pending.length ?? 0),
      waiting: (sessionID) =>
        Effect.sync(() =>
          Array.from(active.get(sessionID)?.requests.values() ?? []).reduce(
            (total, request) => total + request.waiters,
            0,
          ),
        ),
    })
  }),
)

export const node = makeGlobalNode({ service: Service, layer, deps: [] })
