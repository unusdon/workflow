import type { Event, WorkflowRun } from '@workflow/world';
import type { DecryptionKey } from './serialization/encryption.js';
import {
  type PreparedReplayPayload,
  prepareReplayPayload,
} from './serialization/replay.js';

const WORKFLOW_INPUT = Symbol('workflow-input');
const MAX_MEMOIZED_PRIMITIVE_CHARACTERS = 16 * 1024 * 1024;
type ReplayPayloadKey = string | typeof WORKFLOW_INPUT;

/** Copy a view only when retaining it would also retain unrelated bytes. */
function compactOwnedBytes(data: Uint8Array): Uint8Array {
  return data.byteOffset === 0 && data.byteLength === data.buffer.byteLength
    ? data
    : data.slice();
}

function cacheablePrimitiveCharacters(value: unknown): number | undefined {
  const type = typeof value;
  if (value === null) return 0;
  if (type === 'string') {
    return (value as string).length;
  }
  if (type === 'bigint') {
    return (value as bigint).toString().length;
  }
  if (type === 'object' || type === 'function' || type === 'symbol') {
    return undefined;
  }
  return 0;
}

/**
 * Invocation-scoped cache for replay payload hydration.
 *
 * The cache retains VM-independent decrypt/decompress output across fresh VMs.
 * Deserialization still runs against each VM's globals so object graphs and
 * Workflow objects remain realm-local. Primitive final values are safe to
 * share and skip that repeated deserialization entirely, within a bounded
 * character budget because their prepared bytes remain cached too.
 *
 * Key lookup is deliberately outside this class. The runtime creates the
 * cache once the run's key has resolved, then feeds it decoded events.
 */
export class ReplayPayloadCache {
  private readonly preparations = new Map<
    ReplayPayloadKey,
    Promise<Uint8Array>
  >();
  private readonly primitiveValues = new Map<string, unknown>();
  private memoizedPrimitiveCharacters = 0;
  private nextUnpreparedEventIndex = 0;

  constructor(
    private readonly encryptionKey?: DecryptionKey,
    private readonly preparer: typeof prepareReplayPayload = prepareReplayPayload
  ) {}

  /** Prepare a payload as soon as its event frame has been decoded. */
  prepareEvent(event: Event): void {
    switch (event.eventType) {
      case 'run_created':
        this.cachePayload(WORKFLOW_INPUT, event.eventData.input);
        break;
      case 'run_started':
        this.cachePayload(WORKFLOW_INPUT, event.eventData?.input);
        break;
      case 'step_completed':
        this.cachePayload(event.eventId, event.eventData?.result);
        break;
      case 'step_failed':
        this.cachePayload(event.eventId, event.eventData?.error);
        break;
      case 'hook_received':
        this.cachePayload(event.eventId, event.eventData?.payload);
    }
  }

  /** Prepare every payload not already seen through the event stream. */
  prepareAll(workflowRun: WorkflowRun, events: Event[]): void {
    this.cachePayload(WORKFLOW_INPUT, workflowRun.input);
    for (
      let index = this.nextUnpreparedEventIndex;
      index < events.length;
      index++
    ) {
      this.prepareEvent(events[index]);
    }
    this.nextUnpreparedEventIndex = events.length;
  }

  /** Rescan after an event log is replaced or reordered. */
  resetScan(): void {
    this.nextUnpreparedEventIndex = 0;
  }

  getWorkflowInput(
    workflowRun: WorkflowRun
  ): PreparedReplayPayload | Promise<PreparedReplayPayload> {
    return this.getPayload(WORKFLOW_INPUT, workflowRun.input);
  }

  getEventValue(
    eventId: string,
    serializedValue: unknown,
    hydrate: (prepared: PreparedReplayPayload) => unknown | Promise<unknown>
  ): unknown | Promise<unknown> {
    if (this.primitiveValues.has(eventId)) {
      return this.primitiveValues.get(eventId);
    }

    const prepared = this.getPayload(eventId, serializedValue);
    const hydrateAndCache = (payload: PreparedReplayPayload) => {
      const hydrated = hydrate(payload);
      return hydrated instanceof Promise
        ? hydrated.then((value) => this.cachePrimitive(eventId, value))
        : this.cachePrimitive(eventId, hydrated);
    };
    return prepared instanceof Promise
      ? prepared.then(hydrateAndCache)
      : hydrateAndCache(prepared);
  }

  private cachePrimitive(eventId: string, value: unknown): unknown {
    const characters = cacheablePrimitiveCharacters(value);
    if (
      characters === undefined ||
      this.memoizedPrimitiveCharacters + characters >
        MAX_MEMOIZED_PRIMITIVE_CHARACTERS
    ) {
      return value;
    }
    this.primitiveValues.set(eventId, value);
    this.memoizedPrimitiveCharacters += characters;
    return value;
  }

  private cachePayload(cacheKey: ReplayPayloadKey, value: unknown): void {
    if (!(value instanceof Uint8Array) || this.preparations.has(cacheKey)) {
      return;
    }

    const preparation = this.preparer(value, this.encryptionKey).then(
      compactOwnedBytes
    );
    this.preparations.set(cacheKey, preparation);
    void preparation.catch(() => {});
  }

  private getPayload(
    cacheKey: ReplayPayloadKey,
    value: unknown
  ): PreparedReplayPayload | Promise<PreparedReplayPayload> {
    if (!(value instanceof Uint8Array)) return { legacy: value };

    this.cachePayload(cacheKey, value);
    const prepared = this.preparations.get(cacheKey);
    if (!prepared) {
      throw new Error('Replay payload preparation was not cached');
    }

    return prepared.catch((error) => {
      if (this.preparations.get(cacheKey) === prepared) {
        this.preparations.delete(cacheKey);
      }
      throw error;
    });
  }
}
