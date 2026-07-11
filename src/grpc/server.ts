import * as grpc from '@grpc/grpc-js'
import * as protoLoader from '@grpc/proto-loader'
import path from 'path'
import { randomUUID } from 'crypto'
import { QueryEngine } from '../QueryEngine.js'
import { getTools } from '../tools.js'
import { getDefaultAppState } from '../state/AppStateStore.js'
import { AppState } from '../state/AppState.js'
import { FileStateCache, READ_FILE_STATE_CACHE_SIZE } from '../utils/fileStateCache.js'
import { getBuiltInAgents } from '../tools/AgentTool/builtInAgents.js'
import {
  getActiveProviderProfile,
  getProviderProfiles,
  applyProviderProfileToProcessEnv,
} from '../utils/providerProfiles.js'
import type { ProviderProfile } from '../utils/config.js'

const PROTO_PATH = path.resolve(import.meta.dirname, '../proto/openclaude.proto')

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
})

const protoDescriptor = grpc.loadPackageDefinition(packageDefinition) as any
const openclaudeProto = protoDescriptor.openclaude.v1

const MAX_SESSIONS = 1000

// --- Per-request model -> provider routing -------------------------------
//
// The process env (OPENAI_BASE_URL / OPENAI_API_KEY / OPENAI_MODEL, etc.) is
// read fresh at request time by the OpenAI-compatible transport, but it is
// process-wide state: only one provider profile's env can be "active" at a
// time. This server normally just forwards req.model into the already-active
// profile's endpoint (fine when the requested model belongs to that
// profile — e.g. switching between GLM-5.2/GLM-5-Turbo on the same Z.AI
// account). To let a single long-running server also answer for a model that
// lives behind a *different* saved provider profile (e.g. deepseek-v4-flash
// via the opencode zen custom provider while GLM-5.2 stays the default), we
// temporarily swap the process env to that profile for the duration of the
// request, then swap back to the boot-time active profile afterwards.
//
// Because this mutates global process.env, concurrent Chat streams are
// serialized through swapEnvLock so two in-flight requests can never see
// each other's provider env mid-swap.

let bootProfileResolved = false
let bootProfile: ProviderProfile | undefined

function getBootProfile(): ProviderProfile | undefined {
  if (!bootProfileResolved) {
    bootProfile = getActiveProviderProfile()
    bootProfileResolved = true
  }
  return bootProfile
}

/** Find a saved provider profile whose `model` list contains the given model id. */
function findProfileForModel(model: string): ProviderProfile | undefined {
  const target = model.trim().toLowerCase()
  if (!target) return undefined
  return getProviderProfiles().find((profile) =>
    (profile.model || '')
      .split(',')
      .map((m) => m.trim().toLowerCase())
      .includes(target),
  )
}

let swapLockTail: Promise<unknown> = Promise.resolve()

/** Serializes `fn` behind any other pending env-sensitive request. */
function withEnvLock<T>(fn: () => Promise<T>): Promise<T> {
  const run = swapLockTail.then(fn, fn)
  swapLockTail = run.then(
    () => undefined,
    () => undefined,
  )
  return run
}

export class GrpcServer {
  private server: grpc.Server
  private sessions: Map<string, any[]> = new Map()

  constructor() {
    this.server = new grpc.Server()
    this.server.addService(openclaudeProto.AgentService.service, {
      Chat: this.handleChat.bind(this),
    })
  }

  start(port: number = 50051, host: string = 'localhost') {
    this.server.bindAsync(
      `${host}:${port}`,
      grpc.ServerCredentials.createInsecure(),
      (error, boundPort) => {
        if (error) {
          console.error('Failed to start gRPC server')
          return
        }
        console.log(`gRPC Server running at ${host}:${boundPort}`)
      }
    )
  }

  private handleChat(call: grpc.ServerDuplexStream<any, any>) {
    let engine: QueryEngine | null = null
    let appState: AppState = getDefaultAppState()
    const fileCache: FileStateCache = new FileStateCache(READ_FILE_STATE_CACHE_SIZE, 25 * 1024 * 1024)

    // To handle ActionRequired (ask user for permission)
    const pendingRequests = new Map<string, (reply: string) => void>()

    // Accumulated messages from previous turns for multi-turn context
    let previousMessages: any[] = []
    let sessionId = ''
    let interrupted = false

    call.on('data', async (clientMessage) => {
      try {
        if (clientMessage.request) {
          if (engine) {
            call.write({
              error: {
                message: 'A request is already in progress on this stream',
                code: 'ALREADY_EXISTS'
              }
            })
            return
          }
          interrupted = false
          const req = clientMessage.request
          sessionId = req.session_id || ''
          previousMessages = []

          // Load previous messages from session store (cross-stream persistence)
          if (sessionId && this.sessions.has(sessionId)) {
            previousMessages = [...this.sessions.get(sessionId)!]
          }

          const toolNameById = new Map<string, string>()

          // Resolve whether req.model requires temporarily routing this
          // request to a different saved provider profile (see withEnvLock
          // doc comment above). Falls through to whatever profile is already
          // active when req.model is empty or belongs to it already.
          const requestedModel = typeof req.model === 'string' ? req.model.trim() : ''
          const targetProfile = requestedModel ? findProfileForModel(requestedModel) : undefined
          const boot = getBootProfile()
          const needsProviderSwap = !!targetProfile && targetProfile.id !== boot?.id

          await withEnvLock(async () => {
          try {
          if (needsProviderSwap && targetProfile) {
            applyProviderProfileToProcessEnv(targetProfile)
          }

          engine = new QueryEngine({
            cwd: req.working_directory || process.cwd(),
            tools: getTools(appState.toolPermissionContext), // Gets all available tools
            commands: [], // Slash commands
            mcpClients: [],
            // Register OpenClaude's built-in agents (general-purpose,
            // statusline-setup, optional code-guide). Without this the Agent
            // tool throws "Agent type 'general-purpose' not found" the moment
            // the model tries to spawn a subagent for investigation.
            agents: getBuiltInAgents(),
            ...(previousMessages.length > 0 ? { initialMessages: previousMessages } : {}),
            includePartialMessages: true,
            canUseTool: async (tool, input, context, assistantMsg, toolUseID) => {
              if (toolUseID) {
                toolNameById.set(toolUseID, tool.name)
              }
              // Notify client of the tool call first
              call.write({
                tool_start: {
                  tool_name: tool.name,
                  arguments_json: JSON.stringify(input),
                  tool_use_id: toolUseID
                }
              })

              // Bypass mode: auto-allow every tool without emitting ActionRequired.
              // Used by headless callers (e.g. the MCP bridge) that run within a
              // single request/response turn and cannot answer a permission prompt.
              if (req.bypass_permissions) {
                return { behavior: 'allow' }
              }

              // Ask user for permission
              const promptId = randomUUID()
              const question = `Approve ${tool.name}?`
              call.write({
                action_required: {
                  prompt_id: promptId,
                  question,
                  type: 'CONFIRM_COMMAND'
                }
              })

              return new Promise((resolve) => {
                pendingRequests.set(promptId, (reply) => {
                  if (reply.toLowerCase() === 'yes' || reply.toLowerCase() === 'y') {
                    resolve({ behavior: 'allow' })
                  } else {
                    resolve({ behavior: 'deny', reason: 'User denied via gRPC' })
                  }
                })
              })
            },
            getAppState: () => appState,
            setAppState: (updater) => { appState = updater(appState) },
            readFileCache: fileCache,
            userSpecifiedModel: req.model,
            fallbackModel: req.model,
            ...(req.system_prompt ? { customSystemPrompt: req.system_prompt } : {}),
          })

          // Track accumulated response data for FinalResponse
          let fullText = ''
          let promptTokens = 0
          let completionTokens = 0

          const generator = engine.submitMessage(req.message)

          for await (const msg of generator) {
            if (msg.type === 'stream_event') {
              if (msg.event.type === 'content_block_delta' && msg.event.delta.type === 'text_delta') {
                call.write({
                  text_chunk: {
                    text: msg.event.delta.text
                  }
                })
                fullText += msg.event.delta.text
              }
            } else if (msg.type === 'user') {
              // Extract tool results
              const content = msg.message.content
              if (Array.isArray(content)) {
                for (const block of content) {
                  if (block.type === 'tool_result') {
                    let outputStr = ''
                    if (typeof block.content === 'string') {
                      outputStr = block.content
                    } else if (Array.isArray(block.content)) {
                      outputStr = block.content.map(c => c.type === 'text' ? c.text : '').join('\n')
                    }
                    call.write({
                      tool_result: {
                        tool_name: toolNameById.get(block.tool_use_id) ?? block.tool_use_id,
                        tool_use_id: block.tool_use_id,
                        output: outputStr,
                        is_error: block.is_error || false
                      }
                    })
                  }
                }
              }
            } else if (msg.type === 'result') {
              // Extract real token counts and final text from the result
              if (msg.subtype === 'success') {
                if (msg.result) {
                  fullText = msg.result
                }
                promptTokens = msg.usage?.input_tokens ?? 0
                completionTokens = msg.usage?.output_tokens ?? 0
              }
            }
          }

          if (!interrupted) {
            // Save messages for multi-turn context in subsequent requests
            previousMessages = [...engine.getMessages()]

            // Persist to session store for cross-stream resumption
            if (sessionId) {
              if (!this.sessions.has(sessionId) && this.sessions.size >= MAX_SESSIONS) {
                // Evict oldest session (Map preserves insertion order)
                this.sessions.delete(this.sessions.keys().next().value)
              }
              this.sessions.set(sessionId, previousMessages)
            }

            call.write({
              done: {
                full_text: fullText,
                prompt_tokens: promptTokens,
                completion_tokens: completionTokens
              }
            })
          }

          engine = null
          } finally {
            // Always restore the boot-time active profile's env so the next
            // request (which may not specify a model at all) keeps hitting
            // the expected default endpoint.
            if (needsProviderSwap && boot) {
              applyProviderProfileToProcessEnv(boot)
            }
          }
          })

        } else if (clientMessage.input) {
          const promptId = clientMessage.input.prompt_id
          const reply = clientMessage.input.reply
          if (pendingRequests.has(promptId)) {
            pendingRequests.get(promptId)!(reply)
            pendingRequests.delete(promptId)
          }
        } else if (clientMessage.cancel) {
          interrupted = true
          if (engine) {
            engine.interrupt()
          }
          call.end()
        }
      } catch (err: any) {
        console.error('Error processing stream')
        call.write({
          error: {
            message: err.message || "Internal server error",
            code: "INTERNAL"
          }
        })
        call.end()
      }
    })

    call.on('end', () => {
      interrupted = true
      // Unblock any pending permission prompts so canUseTool can return
      for (const resolve of pendingRequests.values()) {
        resolve('no')
      }
      if (engine) {
        engine.interrupt()
      }
      engine = null
      pendingRequests.clear()
    })
  }
}
