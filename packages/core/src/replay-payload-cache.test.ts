import type { Event, WorkflowRun } from '@workflow/world';
import { assert, describe, expect, it, vi } from 'vitest';
import { importKey } from './encryption.js';
import { ReplayPayloadCache } from './replay-payload-cache.js';
import { prepareReplayPayload } from './serialization/replay.js';
import {
  dehydrateStepReturnValue,
  deserializePreparedReplayPayload,
} from './serialization.js';

function makeRun(input: unknown): WorkflowRun {
  const now = new Date();
  return {
    runId: 'wrun_cache_test',
    status: 'running',
    deploymentId: 'dpl_test',
    workflowName: 'workflow//test//cache',
    input,
    attributes: {},
    startedAt: now,
    createdAt: now,
    updatedAt: now,
  };
}

function makeEvents(payloads: unknown[]): Event[] {
  const createdAt = new Date();
  return [
    {
      runId: 'wrun_cache_test',
      eventId: 'evnt_result',
      eventType: 'step_completed',
      correlationId: 'step_result',
      eventData: { result: payloads[0] },
      createdAt,
    },
    {
      runId: 'wrun_cache_test',
      eventId: 'evnt_error',
      eventType: 'step_failed',
      correlationId: 'step_error',
      eventData: { error: payloads[1] },
      createdAt,
    },
    {
      runId: 'wrun_cache_test',
      eventId: 'evnt_hook',
      eventType: 'hook_received',
      correlationId: 'hook_payload',
      eventData: { payload: payloads[2] },
      createdAt,
    },
  ];
}

describe('ReplayPayloadCache', () => {
  it('deduplicates preparation', async () => {
    const payload = new Uint8Array([1]);
    const preparer = vi.fn<typeof prepareReplayPayload>(async (value) => value);
    const hydrate = vi.fn((prepared: unknown) => prepared);
    const cache = new ReplayPayloadCache(undefined, preparer);

    const first = cache.getEventValue('evnt_one', payload, hydrate);
    const second = cache.getEventValue('evnt_one', payload, hydrate);

    await expect(first).resolves.toBe(payload);
    await expect(second).resolves.toBe(payload);
    expect(preparer).toHaveBeenCalledOnce();
    expect(hydrate).toHaveBeenCalledTimes(2);
  });

  it('compacts prepared bytes before retaining them', async () => {
    const backing = new Uint8Array(64 * 1024);
    const prepared = backing.subarray(1024, 2048);
    const cache = new ReplayPayloadCache(
      undefined,
      vi.fn<typeof prepareReplayPayload>(async () => prepared)
    );

    const retained = await cache.getEventValue(
      'evnt_compact',
      new Uint8Array([1]),
      (value) => value
    );

    assert(retained instanceof Uint8Array);
    expect(retained).toEqual(prepared);
    expect(retained.buffer.byteLength).toBe(retained.byteLength);
  });

  it('hydrates and memoizes a primitive', async () => {
    const payload = new Uint8Array([1]);
    const hydrate = vi.fn(() => 42);
    const cache = new ReplayPayloadCache(undefined, async (value) => value);

    await expect(
      cache.getEventValue('evnt_one', payload, hydrate)
    ).resolves.toBe(42);
    expect(cache.getEventValue('evnt_one', payload, hydrate)).toBe(42);
    expect(hydrate).toHaveBeenCalledOnce();
  });

  it('keeps a failed prewarm until its consumer observes it, then retries', async () => {
    const payload = new Uint8Array([1]);
    const run = makeRun(payload);
    const preparer = vi
      .fn<typeof prepareReplayPayload>()
      .mockRejectedValueOnce(new Error('decrypt failed'))
      .mockResolvedValueOnce(payload);
    const cache = new ReplayPayloadCache(undefined, preparer);

    cache.prepareAll(run, []);
    await Promise.resolve();
    await expect(cache.getWorkflowInput(run)).rejects.toThrow('decrypt failed');
    expect(preparer).toHaveBeenCalledOnce();

    await expect(cache.getWorkflowInput(run)).resolves.toBe(payload);
    expect(preparer).toHaveBeenCalledTimes(2);
  });

  it('prewarms workflow, step, error, and hook payloads concurrently', async () => {
    const payloads = [0, 1, 2, 3].map((value) => new Uint8Array([value]));
    const resolvers: Array<() => void> = [];
    const preparer = vi.fn<typeof prepareReplayPayload>(
      (value) =>
        new Promise((resolve) => {
          resolvers.push(() => resolve(value));
        })
    );
    const cache = new ReplayPayloadCache(undefined, preparer);
    const run = makeRun(payloads[0]);
    const events = makeEvents(payloads.slice(1));

    cache.prepareAll(run, events);
    expect(preparer).toHaveBeenCalledTimes(4);
    for (const resolve of resolvers.reverse()) resolve();
    await Promise.all([
      cache.getWorkflowInput(run),
      ...events.map((event) => {
        switch (event.eventType) {
          case 'step_completed':
            return cache.getEventValue(
              event.eventId,
              event.eventData?.result,
              (prepared) => prepared
            );
          case 'step_failed':
            return cache.getEventValue(
              event.eventId,
              event.eventData?.error,
              (prepared) => prepared
            );
          case 'hook_received':
            return cache.getEventValue(
              event.eventId,
              event.eventData?.payload,
              (prepared) => prepared
            );
          default:
            throw new Error(`Unexpected event: ${event.eventType}`);
        }
      }),
    ]);

    cache.prepareAll(run, events);
    expect(preparer).toHaveBeenCalledTimes(4);
  });

  it('starts streamed preparation inside the decoder callback', async () => {
    const payload = new Uint8Array([1]);
    const order: string[] = [];
    const preparer = vi.fn<typeof prepareReplayPayload>(async (value) => {
      order.push('prepare');
      return value;
    });
    const cache = new ReplayPayloadCache(undefined, preparer);
    const [event] = makeEvents([payload]);

    cache.prepareEvent(event);
    expect(preparer).toHaveBeenCalledOnce();
    expect(order).toEqual(['prepare']);

    cache.prepareEvent(event);
    expect(order).toEqual(['prepare']);

    await expect(
      cache.getEventValue(event.eventId, payload, (prepared) => prepared)
    ).resolves.toBe(payload);
  });

  it('caches real decrypt/decompress output but revives fresh objects', async () => {
    const key = await importKey(new Uint8Array(32).fill(7));
    const serialized = await dehydrateStepReturnValue(
      { count: 0, text: 'compressible'.repeat(200) },
      'wrun_cache_test',
      key,
      [],
      globalThis,
      false,
      false,
      true
    );
    const preparer = vi.fn<typeof prepareReplayPayload>(prepareReplayPayload);
    const cache = new ReplayPayloadCache(key, preparer);

    const directPreparation = prepareReplayPayload(serialized, key);
    expect(directPreparation).toBeInstanceOf(Promise);
    await directPreparation;

    const prepared = await cache.getEventValue(
      'evnt_encrypted',
      serialized,
      (value) => value
    );
    const samePrepared = await cache.getEventValue(
      'evnt_encrypted',
      serialized,
      (value) => value
    );
    const first = deserializePreparedReplayPayload(prepared) as {
      count: number;
    };
    first.count = 99;
    const second = deserializePreparedReplayPayload(samePrepared) as {
      count: number;
    };

    expect(preparer).toHaveBeenCalledOnce();
    expect(second).not.toBe(first);
    expect(second.count).toBe(0);
  });

  it('prepares only events appended after the scanned prefix', () => {
    const payloads = [0, 1, 2].map((value) => new Uint8Array([value]));
    const preparer = vi.fn<typeof prepareReplayPayload>(async (value) => value);
    const cache = new ReplayPayloadCache(undefined, preparer);
    const run = makeRun(undefined);
    const [first, second, third] = makeEvents(payloads);
    const prepareEvent = vi.spyOn(cache, 'prepareEvent');

    cache.prepareAll(run, [first, second]);
    expect(preparer).toHaveBeenCalledTimes(2);

    prepareEvent.mockClear();
    cache.prepareAll(run, [first, second, third]);
    expect(prepareEvent).toHaveBeenCalledOnce();
    expect(prepareEvent).toHaveBeenCalledWith(third);
    expect(preparer).toHaveBeenCalledTimes(3);
  });

  it('rescans a corrected event log after reset', () => {
    const payloads = [0, 1, 2].map((value) => new Uint8Array([value]));
    const preparer = vi.fn<typeof prepareReplayPayload>(async (value) => value);
    const cache = new ReplayPayloadCache(undefined, preparer);
    const run = makeRun(undefined);
    const [first, missing, second] = makeEvents(payloads);

    cache.prepareAll(run, [first, second]);
    cache.prepareAll(run, [first, missing, second]);
    expect(preparer).toHaveBeenCalledTimes(2);

    cache.resetScan();
    cache.prepareAll(run, [first, missing, second]);
    expect(preparer).toHaveBeenCalledTimes(3);
    expect(preparer).toHaveBeenLastCalledWith(payloads[1], undefined);
  });

  it('bypasses legacy values and ignores missing event data during preparation', async () => {
    const legacy = [0, { value: 1 }];
    const preparer = vi.fn<typeof prepareReplayPayload>(async (value) => value);
    const cache = new ReplayPayloadCache(undefined, preparer);

    await cache.getEventValue('evnt_legacy', legacy, (prepared) => prepared);
    await cache.getEventValue('evnt_legacy', legacy, (prepared) => prepared);
    expect(preparer).not.toHaveBeenCalled();

    const events = makeEvents([legacy, legacy, legacy]);
    events[2] = { ...events[2], eventData: undefined } as unknown as Event;
    cache.prepareAll(makeRun(legacy), events);
    expect(preparer).not.toHaveBeenCalled();
  });

  it('memoizes primitive step results, including undefined', async () => {
    for (const value of [0, false, '', null, undefined]) {
      const cache = new ReplayPayloadCache();
      const hydrate = vi.fn().mockResolvedValue(value);

      expect(await cache.getEventValue('evnt_result', undefined, hydrate)).toBe(
        value
      );
      expect(await cache.getEventValue('evnt_result', undefined, hydrate)).toBe(
        value
      );
      expect(hydrate).toHaveBeenCalledOnce();
    }
  });

  it('isolates primitive values by event id', async () => {
    const cache = new ReplayPayloadCache();
    const result = vi.fn().mockResolvedValue('result');
    const error = vi.fn().mockResolvedValue('error');

    await expect(
      cache.getEventValue('evnt_result', undefined, result)
    ).resolves.toBe('result');
    await expect(
      cache.getEventValue('evnt_error', undefined, error)
    ).resolves.toBe('error');
    expect(cache.getEventValue('evnt_result', undefined, result)).toBe(
      'result'
    );
    expect(result).toHaveBeenCalledOnce();
    expect(error).toHaveBeenCalledOnce();
  });

  it('memoizes primitives within the budget and rehydrates larger results', async () => {
    const commonText = 'x'.repeat(256 * 1024);
    const oversizedText = 'x'.repeat(16 * 1024 * 1024 + 1);
    for (const [value, expectedHydrations] of [
      [{ count: 0 }, 2],
      [commonText, 1],
      [oversizedText, 2],
    ] as const) {
      const cache = new ReplayPayloadCache();
      const hydrate = vi
        .fn()
        .mockImplementation(async () =>
          typeof value === 'object' ? { ...value } : value
        );

      const first = await cache.getEventValue(
        'evnt_result',
        undefined,
        hydrate
      );
      const second = await cache.getEventValue(
        'evnt_result',
        undefined,
        hydrate
      );
      expect(hydrate).toHaveBeenCalledTimes(expectedHydrations);
      if (typeof value === 'object') {
        expect(second).not.toBe(first);
      } else if (expectedHydrations === 1) {
        expect(second).toBe(first);
      }
    }
  });

  it('does not memoize failed step hydration', async () => {
    const cache = new ReplayPayloadCache();
    const hydrate = vi
      .fn()
      .mockRejectedValueOnce(new Error('boom'))
      .mockResolvedValueOnce('ok');

    await expect(
      cache.getEventValue('evnt_result', undefined, hydrate)
    ).rejects.toThrow('boom');
    await expect(
      cache.getEventValue('evnt_result', undefined, hydrate)
    ).resolves.toBe('ok');
    expect(hydrate).toHaveBeenCalledTimes(2);
  });
});
