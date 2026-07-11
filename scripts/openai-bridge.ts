// OpenAI-compatible HTTP bridge for the OpenClaude gRPC agent.
//
// Exposes POST /v1/chat/completions (+ GET /v1/models) and translates each
// request into a single AgentService.Chat gRPC stream with
// bypass_permissions=true, so the agent is selectable as a "model" from any
// OpenAI-compatible client — in particular LiteLLM at llm.scrya.com, which
// routes `openclaude-agent` here.
//
//   bun run scripts/openai-bridge.ts
//
// Env:
//   BRIDGE_HOST (default 0.0.0.0)   BRIDGE_PORT (default 8091)
//   GRPC_HOST   (default localhost) GRPC_PORT   (default 50051)
//   OPENCLAUDE_WORKDIR — agent working dir (default ~/openclaude-bridge-workspace)

import * as grpc from '@grpc/grpc-js'
import * as protoLoader from '@grpc/proto-loader'
import path from 'path'
import os from 'os'
import { mkdirSync } from 'fs'

const PROTO_PATH = path.resolve(import.meta.dirname, '../src/proto/openclaude.proto')
const BRIDGE_HOST = process.env.BRIDGE_HOST || '0.0.0.0'
const BRIDGE_PORT = Number(process.env.BRIDGE_PORT || 8091)
const GRPC_HOST = process.env.GRPC_HOST || 'localhost'
const GRPC_PORT = process.env.GRPC_PORT || '50051'
const MODEL_ID = 'openclaude-agent'
// Models the gRPC server can actually route to (see src/grpc/server.ts
// findProfileForModel): the agent default (GLM-5.2 via the active Z.AI
// profile) plus any other saved provider profile's model id — currently
// deepseek-v4-flash via the opencode zen custom provider.
const PASSTHROUGH_MODELS = ['GLM-5.2', 'deepseek-v4-flash']
const KNOWN_MODEL_IDS = [MODEL_ID, ...PASSTHROUGH_MODELS]
const WORKDIR =
  process.env.OPENCLAUDE_WORKDIR || path.join(os.homedir(), 'openclaude-bridge-workspace')
mkdirSync(WORKDIR, { recursive: true })

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
})
const proto = (grpc.loadPackageDefinition(packageDefinition) as any).openclaude.v1
const agent = new proto.AgentService(
  `${GRPC_HOST}:${GRPC_PORT}`,
  grpc.credentials.createInsecure(),
)

interface OpenAIMessage {
  role: string
  content: string | Array<{ type: string; text?: string }>
}

function contentText(c: OpenAIMessage['content']): string {
  if (typeof c === 'string') return c
  return c.map((p) => p.text ?? '').join('')
}

/** Split an OpenAI messages array into a system prompt + one agent task message. */
function flatten(messages: OpenAIMessage[]): { system: string; task: string } {
  const system = messages
    .filter((m) => m.role === 'system')
    .map((m) => contentText(m.content))
    .join('\n\n')
  const turns = messages.filter((m) => m.role !== 'system')
  const parts: string[] = []
  if (turns.length > 1) {
    parts.push(
      'Conversation so far:\n' +
        turns
          .slice(0, -1)
          .map((m) => `${m.role === 'user' ? 'User' : 'Assistant'}: ${contentText(m.content)}`)
          .join('\n'),
    )
  }
  const last = turns[turns.length - 1]
  if (last) parts.push(contentText(last.content))
  return { system, task: parts.join('\n\n') }
}

interface AgentEvents {
  onText: (t: string) => void
  onDone: (fullText: string) => void
  onError: (message: string) => void
}

/**
 * Maps the OpenAI "model" field to the gRPC ChatRequest.model.
 *  - "openclaude-agent" (or empty/unrecognized) -> omit, agent default (GLM-5.2)
 *  - optional "openclaude:" prefix is stripped
 *  - anything else is passed through verbatim; the gRPC server resolves it
 *    against saved provider profiles (see src/grpc/server.ts) and 400s
 *    upstream if it doesn't recognize it.
 */
function resolveGrpcModel(requestedModel: unknown): string | undefined {
  if (typeof requestedModel !== 'string') return undefined
  const stripped = requestedModel.trim().replace(/^openclaude:/i, '')
  if (!stripped || stripped === MODEL_ID) return undefined
  return stripped
}

/** One request = one gRPC Chat stream. Returns a cancel function. */
function runAgent(
  message: string,
  systemPrompt: string,
  sessionId: string,
  model: string | undefined,
  ev: AgentEvents,
): () => void {
  const call = agent.Chat()
  let streamed = ''

  call.on('data', (msg: any) => {
    if (msg.text_chunk) {
      streamed += msg.text_chunk.text
      ev.onText(msg.text_chunk.text)
    } else if (msg.action_required) {
      // bypass_permissions makes this unreachable in practice; auto-approve
      // defensively so a caller is never left hanging mid-stream.
      call.write({ input: { prompt_id: msg.action_required.prompt_id, reply: 'y' } })
    } else if (msg.done) {
      const full = streamed || msg.done.full_text || ''
      call.end()
      ev.onDone(full)
    } else if (msg.error) {
      call.end()
      ev.onError(msg.error.message || 'agent error')
    }
  })
  call.on('error', (e: Error) => ev.onError(e.message))

  call.write({
    request: {
      message,
      working_directory: WORKDIR,
      session_id: sessionId,
      bypass_permissions: true,
      ...(model ? { model } : {}),
      ...(systemPrompt ? { system_prompt: systemPrompt } : {}),
    },
  })
  return () => {
    try {
      call.cancel()
    } catch {}
  }
}

const completionId = () => `chatcmpl-${crypto.randomUUID().slice(0, 12)}`
const now = () => Math.floor(Date.now() / 1000)

function completionJson(id: string, text: string, model: string) {
  return {
    id,
    object: 'chat.completion',
    created: now(),
    model,
    choices: [
      {
        index: 0,
        message: { role: 'assistant', content: text },
        finish_reason: 'stop',
      },
    ],
    usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
  }
}

function sseChunk(id: string, model: string, delta: object, finish: string | null = null) {
  return `data: ${JSON.stringify({
    id,
    object: 'chat.completion.chunk',
    created: now(),
    model,
    choices: [{ index: 0, delta, finish_reason: finish }],
  })}\n\n`
}

Bun.serve({
  hostname: BRIDGE_HOST,
  port: BRIDGE_PORT,
  idleTimeout: 255, // agent turns are slow — max Bun allows
  async fetch(req) {
    const url = new URL(req.url)

    if (url.pathname === '/health') {
      return Response.json({ status: 'ok', model: MODEL_ID, grpc: `${GRPC_HOST}:${GRPC_PORT}` })
    }

    if (url.pathname === '/v1/models') {
      return Response.json({
        object: 'list',
        data: KNOWN_MODEL_IDS.map((id) => ({
          id,
          object: 'model',
          created: now(),
          owned_by: 'openclaude',
        })),
      })
    }

    if (url.pathname === '/v1/chat/completions' && req.method === 'POST') {
      let body: any
      try {
        body = await req.json()
      } catch {
        return Response.json({ error: { message: 'invalid JSON body' } }, { status: 400 })
      }
      const messages = (body.messages ?? []) as OpenAIMessage[]
      if (!messages.length) {
        return Response.json({ error: { message: 'messages required' } }, { status: 400 })
      }
      const { system, task } = flatten(messages)
      // OpenAI's `user` field doubles as the cross-request agent session id.
      const sessionId = typeof body.user === 'string' ? body.user : ''
      const id = completionId()
      // What we echo back in responses' "model" field: the id the caller
      // asked for (falling back to the default agent id), independent of
      // whether it maps to a gRPC model override.
      const responseModel =
        typeof body.model === 'string' && body.model.trim() ? body.model.trim() : MODEL_ID
      const grpcModel = resolveGrpcModel(body.model)

      if (body.stream) {
        let cancel: () => void = () => {}
        const stream = new ReadableStream({
          start(controller) {
            const enc = new TextEncoder()
            controller.enqueue(enc.encode(sseChunk(id, responseModel, { role: 'assistant' })))
            cancel = runAgent(task, system, sessionId, grpcModel, {
              onText: (t) => controller.enqueue(enc.encode(sseChunk(id, responseModel, { content: t }))),
              onDone: () => {
                controller.enqueue(enc.encode(sseChunk(id, responseModel, {}, 'stop')))
                controller.enqueue(enc.encode('data: [DONE]\n\n'))
                controller.close()
              },
              onError: (m) => {
                controller.enqueue(
                  enc.encode(sseChunk(id, responseModel, { content: `\n[bridge error: ${m}]` }, 'stop')),
                )
                controller.enqueue(enc.encode('data: [DONE]\n\n'))
                controller.close()
              },
            })
          },
          cancel() {
            cancel()
          },
        })
        return new Response(stream, {
          headers: {
            'Content-Type': 'text/event-stream',
            'Cache-Control': 'no-cache',
            Connection: 'keep-alive',
          },
        })
      }

      const text = await new Promise<string>((resolve, reject) => {
        runAgent(task, system, sessionId, grpcModel, {
          onText: () => {},
          onDone: resolve,
          onError: (m) => reject(new Error(m)),
        })
      }).catch((e) => {
        throw e
      })
      return Response.json(completionJson(id, text, responseModel))
    }

    return Response.json({ error: { message: 'not found' } }, { status: 404 })
  },
})

console.log(
  `OpenClaude OpenAI bridge on http://${BRIDGE_HOST}:${BRIDGE_PORT} -> grpc ${GRPC_HOST}:${GRPC_PORT} (workdir ${WORKDIR})`,
)
