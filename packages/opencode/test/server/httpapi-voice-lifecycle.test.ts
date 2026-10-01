import { afterEach, describe, expect } from "bun:test"
import { spawn } from "node:child_process"
import { Effect, Fiber, Layer, Queue, Schema, Stream } from "effect"
import { HttpServer } from "effect/unstable/http"
import { decode } from "effect/unstable/encoding/Sse"
import { EventV2 } from "@opencode-ai/core/event"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { SessionEvent } from "@opencode-ai/schema/session-event"
import { Permission } from "@opencode-ai/schema/permission"
import { SessionInput } from "@opencode-ai/schema/session-input"
import { SessionSchema } from "@opencode-ai/core/session/schema"
import { SessionWorkControl } from "@opencode-ai/core/session/work-control"
import { resetDatabase } from "../fixture/db"
import { disposeAllInstances, tmpdirScoped } from "../fixture/fixture"
import { TestLLMServer } from "../lib/llm-server"
import { pollWithTimeout, testEffect } from "../lib/effect"
import { httpApiLayer, requestInDirectory } from "./httpapi-layer"

const WireEvent = Schema.Struct({
  id: EventV2.ID,
  type: Schema.String,
  durable: Schema.optional(Schema.Struct({ seq: Schema.Number })),
  data: Schema.Record(Schema.String, Schema.Unknown),
})

const openEvents = Effect.fnUntraced(function* (directory: string, path = "/api/event") {
  const response = yield* requestInDirectory(path, directory)
  expect(response.status).toBe(200)
  expect(response.headers["content-type"]).toContain("text/event-stream")
  const queue = yield* Queue.unbounded<typeof WireEvent.Type>()
  yield* response.stream.pipe(
    Stream.decodeText,
    Stream.pipeThroughChannel(decode()),
    Stream.filter((event) => event._tag === "Event"),
    Stream.map((event) => Schema.decodeUnknownSync(Schema.fromJsonString(WireEvent))(event.data)),
    Stream.runForEach((event) => Queue.offer(queue, event)),
    Effect.forkScoped,
  )
  return queue
})

const nextEvent = Effect.fnUntraced(function* (queue: Queue.Dequeue<typeof WireEvent.Type>, type: string) {
  while (true) {
    const event = yield* Queue.take(queue)
    if (event.type === type) return event
  }
}, Effect.timeout("10 seconds"))

const it = testEffect(httpApiLayer.pipe(Layer.provideMerge(AppNodeBuilder.build(SessionWorkControl.node))))
afterEach(async () => {
  await disposeAllInstances()
  await resetDatabase()
}, 45_000)

describe("voice lifecycle HttpApi", () => {
  it.live(
    "correlates admission, queue holes, work settlement, cancellation, and reconnect replay over HTTP",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        yield* llm.text("Steer completed")
        yield* llm.text("Queue completed")
        const directory = yield* tmpdirScoped({ git: true })
        // Native V2 configuration and synthetic local-only credentials avoid legacy catalog assumptions.
        yield* Effect.promise(() =>
          Bun.write(
            `${directory}/opencode.json`,
            JSON.stringify({
              model: "test/test-model",
              snapshots: false,
              permissions: [{ action: "*", resource: "*", effect: "ask" }],
              formatter: false,
              lsp: false,
              providers: {
                test: {
                  api: { type: "aisdk", package: "@ai-sdk/openai-compatible", url: llm.url, settings: {} },
                  request: { body: { apiKey: "test-key" } },
                  models: {
                    "test-model": {
                      name: "Test",
                      api: { id: "test-model" },
                      capabilities: { tools: true, input: ["text"], output: ["text"] },
                      limit: { context: 100000, output: 10000 },
                    },
                  },
                },
              },
            }),
          ),
        )
        const health = yield* requestInDirectory("/api/health", directory)
        expect(yield* health.json).toEqual({
          healthy: true,
          sessionWorkProtocolVersion: 1,
          sessionWorkControlProtocolVersion: 1,
        })
        const created = yield* requestInDirectory("/api/session", directory, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ location: { directory }, model: { providerID: "test", id: "test-model" } }),
        })
        expect(created.status).toBe(200)
        const body = yield* created.json
        const session = Schema.decodeUnknownSync(Schema.Struct({ data: Schema.Struct({ id: Schema.String }) }))(
          body,
        ).data
        const live = yield* openEvents(directory)
        yield* nextEvent(live, "server.connected")
        const prompt = Effect.fnUntraced(function* (
          id: string,
          text: string,
          delivery: "queue" | "steer",
          resume: boolean,
        ) {
          const response = yield* requestInDirectory(`/api/session/${session.id}/prompt`, directory, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ id, prompt: { text }, delivery, resume }),
          })
          expect(response.status).toBe(200)
          return Schema.decodeUnknownSync(Schema.Struct({ data: SessionInput.Admitted }))(yield* response.json).data
        })
        const queued = yield* prompt("msg_http_voice_queue", "Queued first", "queue", false)
        const steer = yield* prompt("msg_http_voice_steer", "Steer later", "steer", false)
        expect(queued.admittedSeq).toBeLessThan(steer.admittedSeq)
        expect(steer.promotedSeq).toBeUndefined()
        expect(yield* prompt(steer.id, "Steer later", "steer", true)).toEqual(steer)
        const first = yield* nextEvent(live, "session.next.work.settled")
        if (first.data.outcome !== "completed") {
          const diagnostics = yield* requestInDirectory(`/api/session/${session.id}/history?limit=100`, directory)
          return yield* Effect.die({ calls: yield* llm.calls, history: yield* diagnostics.json })
        }
        expect(first.data).toMatchObject({ inputMessageIDs: [steer.id], outcome: "completed" })
        const second = yield* nextEvent(live, "session.next.work.settled")
        expect(second.data).toMatchObject({ inputMessageIDs: [queued.id], outcome: "completed" })
        expect(first.data.workID).not.toBe(second.data.workID)
        expect(first.durable?.seq).toBeLessThan(second.durable?.seq ?? 0)

        yield* llm.hang
        const cancelled = yield* prompt("msg_http_voice_cancel", "Cancel this", "steer", true)
        yield* llm.wait(3)
        const interrupt = yield* requestInDirectory(`/api/session/${session.id}/interrupt`, directory, {
          method: "POST",
        })
        expect(interrupt.status).toBe(204)
        const last = yield* nextEvent(live, "session.next.work.settled")
        expect(last.data).toMatchObject({ inputMessageIDs: [cancelled.id], outcome: "cancelled" })

        const permissionPath = `/api/session/${session.id}/permission`
        const createPermission = (action: string) =>
          requestInDirectory(permissionPath, directory, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ id: "per_http_voice", action, resources: [action], save: [action] }),
          })
        expect((yield* createPermission("voice-first")).status).toBe(200)
        const previousResponse = yield* requestInDirectory(`${permissionPath}/per_http_voice`, directory)
        const previous = Schema.decodeUnknownSync(Schema.Struct({ data: Permission.Request }))(
          yield* previousResponse.json,
        ).data
        const replyPermission = (expectedRequest: Permission.Request) =>
          requestInDirectory(`${permissionPath}/per_http_voice/reply`, directory, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ reply: "once", expectedRequest }),
          })
        expect((yield* replyPermission(previous)).status).toBe(204)
        expect((yield* createPermission("voice-second")).status).toBe(200)
        expect((yield* replyPermission(previous)).status).toBe(409)
        const currentResponse = yield* requestInDirectory(`${permissionPath}/per_http_voice`, directory)
        const current = Schema.decodeUnknownSync(Schema.Struct({ data: Permission.Request }))(
          yield* currentResponse.json,
        ).data
        expect(current.action).toBe("voice-second")
        expect((yield* replyPermission(current)).status).toBe(204)

        // Identical payloads still represent new approval instances when an ID is reused.
        expect((yield* createPermission("voice-second")).status).toBe(200)
        expect((yield* replyPermission(current)).status).toBe(409)
        const replacementResponse = yield* requestInDirectory(`${permissionPath}/per_http_voice`, directory)
        const replacement = Schema.decodeUnknownSync(Schema.Struct({ data: Permission.Request }))(
          yield* replacementResponse.json,
        ).data
        expect(replacement.generation).not.toBe(current.generation)
        expect((yield* replyPermission(replacement)).status).toBe(204)

        // Reconnection uses durable aggregate sequence, not provider IDs or process-local idle state.
        const replay = yield* openEvents(directory, `/api/session/${session.id}/event?after=${first.durable?.seq}`)
        const replayedSecond = yield* nextEvent(replay, "session.next.work.settled")
        const replayedLast = yield* nextEvent(replay, "session.next.work.settled")
        expect(replayedSecond).toEqual(second)
        expect(replayedLast).toEqual(last)
        const history = yield* requestInDirectory(`/api/session/${session.id}/history?limit=100`, directory)
        const events = Schema.decodeUnknownSync(
          Schema.Struct({ data: Schema.Array(SessionEvent.Durable), hasMore: Schema.Boolean }),
        )(yield* history.json)
        expect(events.hasMore).toBe(false)
        expect(events.data.filter((event) => event.type === "session.next.prompt.admitted")).toHaveLength(3)
        expect(
          events.data.filter((event) => event.type === "session.next.work.settled").map((event) => event.id),
        ).toEqual([first.id, second.id, last.id])
        expect(events.data.some((event) => event.type === "session.next.text.ended")).toBe(true)
      }).pipe(
        Effect.provide(TestLLMServer.layer),
        Effect.provide(AppNodeBuilder.build(CrossSpawnSpawner.node)),
        Effect.timeout("30 seconds"),
      ),
    45_000,
  )
  it.live(
    "keeps wire-abort admission uncertain and guarantees explicit work cancellation",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const control = yield* SessionWorkControl.Service
        const server = yield* HttpServer.HttpServer
        if (server.address._tag !== "TcpAddress") return yield* Effect.die("TCP fixture required")
        const address = `http://127.0.0.1:${server.address.port}`
        const held = Promise.withResolvers<void>()
        yield* Effect.addFinalizer(() => Effect.sync(() => held.resolve()))
        yield* llm.hang
        const directory = yield* tmpdirScoped({ git: true })
        yield* Effect.promise(() =>
          Bun.write(
            `${directory}/opencode.json`,
            JSON.stringify({
              model: "test/test-model",
              snapshots: false,
              formatter: false,
              lsp: false,
              providers: {
                test: {
                  api: { type: "aisdk", package: "@ai-sdk/openai-compatible", url: llm.url, settings: {} },
                  request: { body: { apiKey: "test-key" } },
                  models: {
                    "test-model": {
                      name: "Test",
                      api: { id: "test-model" },
                      capabilities: { tools: true, input: ["text"], output: ["text"] },
                      limit: { context: 100000, output: 10000 },
                    },
                  },
                },
              },
            }),
          ),
        )
        const created = yield* requestInDirectory("/api/session", directory, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ location: { directory }, model: { providerID: "test", id: "test-model" } }),
        })
        const session = Schema.decodeUnknownSync(Schema.Struct({ data: Schema.Struct({ id: SessionSchema.ID }) }))(
          yield* created.json,
        ).data
        const base = `/api/session/${session.id}`
        const live = yield* openEvents(directory)
        yield* nextEvent(live, "server.connected")
        const prompt = (id: string, text: string, workID?: string) =>
          requestInDirectory(workID ? `${base}/work/${workID}/prompt` : `${base}/prompt`, directory, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ id, prompt: { text } }),
          })
        expect((yield* prompt("msg_http_fenced_a", "Run A")).status).toBe(200)
        const started = yield* nextEvent(live, "session.next.work.started")
        const workID = started.data.workID
        if (typeof workID !== "string") return yield* Effect.die("A did not start")
        yield* llm.wait(1)
        const abort = new AbortController()
        // Use the actual Node client runtime used by the bridge, not Bun's fetch/HTTP compatibility shim.
        const client = spawn(
          "node",
          [
            "--input-type=module",
            "-e",
            `
          const controller = new AbortController()
          process.stdin.once("data", () => controller.abort())
          try {
            const response = await fetch(process.argv[1], {
              method: "POST", headers: JSON.parse(process.argv[2]), body: process.argv[3], signal: controller.signal,
            })
            console.log(response.status)
          } catch (error) { console.log(error.name) }
          process.stdin.pause()
        `,
            `${address}${base}/work/${workID}/prompt`,
            JSON.stringify({ "content-type": "application/json", "x-opencode-directory": directory }),
            JSON.stringify({ id: "msg_http_aborted", prompt: { text: "Do not admit after abort" } }),
          ],
          { stdio: ["pipe", "pipe", "pipe"] },
        )
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            client.kill()
          }),
        )
        const aborted = new Promise<string>((resolve, reject) => {
          let output = ""
          client.stdout.on("data", (chunk) => {
            output += chunk.toString()
          })
          client.on("error", reject)
          client.on("close", () => resolve(output.trim()))
        })
        abort.signal.addEventListener(
          "abort",
          () => {
            client.stdin.end("abort")
          },
          { once: true },
        )
        yield* pollWithTimeout(
          control.pending(session.id).pipe(Effect.map((count) => (count === 1 ? true : undefined))),
          "HTTP steer did not queue",
        )
        abort.abort()
        expect(yield* Effect.promise(() => aborted)).toBe("AbortError")
        // A wire timeout/abort is uncertain: Bun 1.3.14 may not notify the server of the disconnect.
        // Explicit work cancellation is authoritative and rejects every still-unadmitted queued steer.
        expect(
          (yield* requestInDirectory(`${base}/work/${workID}/interrupt`, directory, { method: "POST" })).status,
        ).toBe(204)
        yield* pollWithTimeout(
          control.pending(session.id).pipe(Effect.map((count) => (count === 0 ? true : undefined))),
          "Explicit interrupt did not remove queued steer",
        )
        expect((yield* nextEvent(live, "session.next.work.settled")).data).toMatchObject({
          workID,
          inputMessageIDs: ["msg_http_fenced_a"],
          outcome: "cancelled",
        })
        yield* llm.hold("B finished", held.promise)
        yield* llm.text("Guarded steer finished")
        expect((yield* prompt("msg_http_fenced_b", "Run B")).status).toBe(200)
        const steerWork = yield* nextEvent(live, "session.next.work.started")
        const steerWorkID = steerWork.data.workID
        if (typeof steerWorkID !== "string") return yield* Effect.die("B did not start")
        yield* llm.wait(2)
        const pending = yield* prompt("msg_http_guarded", "Steer B", steerWorkID).pipe(Effect.forkChild)
        yield* pollWithTimeout(
          control.pending(session.id).pipe(Effect.map((count) => (count === 1 ? true : undefined))),
          "Valid steer did not queue",
        )
        held.resolve()
        const response = yield* Fiber.join(pending)
        expect(response.status).toBe(200)
        const admitted = Schema.decodeUnknownSync(Schema.Struct({ data: SessionInput.Admitted }))(
          yield* response.json,
        ).data
        expect(admitted.promotedSeq).toBe(admitted.admittedSeq)
        const settled = yield* nextEvent(live, "session.next.work.settled")
        expect(settled.data).toMatchObject({
          workID: steerWorkID,
          inputMessageIDs: ["msg_http_fenced_b", "msg_http_guarded"],
          outcome: "completed",
        })
        yield* llm.hang
        expect((yield* prompt("msg_http_fenced_c", "Run C")).status).toBe(200)
        const next = yield* nextEvent(live, "session.next.work.started")
        const nextWorkID = next.data.workID
        if (typeof nextWorkID !== "string") return yield* Effect.die("C did not start")
        yield* llm.wait(4)
        expect(
          (yield* requestInDirectory(`${base}/work/${steerWorkID}/interrupt`, directory, { method: "POST" })).status,
        ).toBe(409)
        expect((yield* prompt("msg_http_stale", "Never steer C", steerWorkID)).status).toBe(409)
        const retry = yield* prompt("msg_http_guarded", "Steer B", steerWorkID)
        expect(retry.status).toBe(200)
        expect(
          Schema.decodeUnknownSync(Schema.Struct({ data: SessionInput.Admitted }))(yield* retry.json).data,
        ).toEqual(admitted)
        expect(
          (yield* requestInDirectory(`${base}/work/${nextWorkID}/interrupt`, directory, { method: "POST" })).status,
        ).toBe(204)
        expect((yield* nextEvent(live, "session.next.work.settled")).data).toMatchObject({
          workID: nextWorkID,
          inputMessageIDs: ["msg_http_fenced_c"],
          outcome: "cancelled",
        })
        const history = yield* requestInDirectory(`${base}/history?limit=100`, directory)
        const encoded = JSON.stringify(yield* history.json)
        expect(encoded).not.toContain("msg_http_aborted")
        expect(encoded).not.toContain("msg_http_stale")
        expect(yield* llm.calls).toBe(4)
      }).pipe(
        Effect.provide(TestLLMServer.layer),
        Effect.provide(AppNodeBuilder.build(CrossSpawnSpawner.node)),
        Effect.timeout("30 seconds"),
      ),
    45_000,
  )
})
