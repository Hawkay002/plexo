import type {
  DownloadStatus,
  HttpBlockState,
  StartTelegramDownloadRequest,
  TelegramDownloadNetwork,
  TelegramReaderState
} from '../../shared/types'
import { describeError } from '../../shared/errors'
import { advanceBlock } from './blockProgress'
import {
  delay,
  recomputeAggregates,
  type Transfer,
  type TransferHost,
  type TelegramTransferTarget
} from './transfer'

/**
 * Where a Telegram file's bytes come from: the telegram module's client, speaking MTProto. One
 * request asks for up to 1 MiB at a 4096-aligned offset — upload.GetFile's own shape — and an
 * empty answer means the end of the file (only a photo's download runs to one: Telegram doesn't
 * say a photo's size ahead of time).
 */
export interface TelegramFileSource {
  /** True when the size is unknown (a photo): requests run until an empty answer. */
  unknownSize: boolean
  request(offset: number, limit: number, signal: AbortSignal): Promise<Buffer>
}

/** Why a request failed, as the transfer acts on it. */
export type TelegramFileErrorKind = 'rate' | 'network' | 'auth' | 'missing' | 'other'

export class TelegramFileError extends Error {
  constructor(
    readonly kind: TelegramFileErrorKind,
    message: string,
    /** For 'rate': how long Telegram asked to be left alone, in seconds. */
    readonly waitSeconds?: number
  ) {
    super(message)
  }
}

/** upload.GetFile answers at most 1 MiB, at offsets and limits divisible by 4096. */
export const FILE_CHUNK_BYTES = 1024 * 1024
/** Concurrent getFile requests, pipelined over the one session. Telegram rate-limits past a
 * handful, and then every request costs a wait. */
export const READER_POOL = 4
const MAX_RETRIES = 8
const RETRY_BASE_MS = 1_000
const RETRY_MAX_MS = 15_000
/** A rate-limit wait longer than this ends the download (resumable) rather than holding it. */
const RATE_WAIT_MAX_MS = 120_000

interface ReaderRuntime {
  id: number
  controller: AbortController
  /** Cuts its waits short when the run stops. */
  wait: AbortSignal
  failures: number
}

type BlockOutcome =
  | { type: 'completed' }
  | { type: 'stopped' }
  | { type: 'retry'; waitMs: number }
  | { type: 'give-up'; error: unknown }

/**
 * Fetches a Telegram file: a small pool of readers, each claiming the first pending block and
 * fetching its remaining bytes in aligned chunks, written at their own offsets into the staging
 * file — the same shape the HTTP transfer writes, so pause/resume, progress and the block grid
 * all work unchanged. Telegram is reached however the OS routes, so a reader's bytes are
 * attributed to whichever network was first-enabled when it claimed its block.
 */
export class TelegramTransfer implements Transfer {
  private workers = new Map<number, Promise<void>>()
  private readers = new Map<number, ReaderRuntime>()
  private readerIds = 0
  private stopped = false
  /** The file source (a promise: it may need a round trip to Telegram to find the file). */
  private sourcePromise: Promise<TelegramFileSource> | undefined

  constructor(
    private readonly runtime: TelegramTransferTarget,
    private readonly host: TransferHost,
    private readonly makeSource: () => Promise<TelegramFileSource> | TelegramFileSource
  ) {}

  private source(): Promise<TelegramFileSource> {
    this.sourcePromise ??= Promise.resolve(this.makeSource())
    return this.sourcePromise
  }

  reconcile(): void {
    if (this.stopped) return
    const { state } = this.runtime
    // A photo's size is unknown, so its one block runs sequentially; everything else splits
    // across the pool.
    const desired = photoDownload(this.runtime.requestPayload)
      ? 1
      : Math.max(
          1,
          Math.min(READER_POOL, state.blocks.filter((block) => block.status !== 'completed').length)
        )
    while (this.workers.size < desired && this.readers.size < desired) {
      this.addReader()
    }
  }

  tick(): void {
    // The manager's reconcile runs from here each tick as it does for HTTP, so a network
    // turned off, a limit reached or a schedule window closing is noticed mid-run.
    this.host.reconcile()
  }

  running(): Iterable<Promise<void>> {
    return this.workers.values()
  }

  abort(): void {
    this.stopped = true
    for (const reader of this.readers.values()) reader.controller.abort()
  }

  reset(): void {
    // A new run starts its pool over; blocks keep what they have on disk.
    for (const reader of this.readers.values()) reader.controller.abort()
    this.readers.clear()
    this.workers.clear()
    this.stopped = false
  }

  wake(): void {
    // Nothing of this transfer's waits on a backoff that a network change should cut short: the
    // readers retry on their own.
  }

  systemResumed(): void {
    // The sockets under the session are likely dead, though nothing says so until a request
    // fails; dropping the in-flight ones makes that happen now rather than after a stall.
    for (const reader of this.readers.values()) reader.controller.abort()
    this.readers.clear()
    this.workers.clear()
    this.stopped = false
  }

  private addReader(): void {
    const { state } = this.runtime
    const controller = new AbortController()
    const reader: TelegramReaderState = {
      id: ++this.readerIds,
      interfaceId: state.networks.find((network) => network.enabled)?.id ?? '',
      bytesDownloaded: 0,
      speedBytesPerSec: 0,
      status: 'pending'
    }
    state.readers.push(reader)
    state.peakReaders = Math.max(state.peakReaders, state.readers.length)
    const self: ReaderRuntime = {
      id: reader.id,
      controller,
      wait: AbortSignal.any([controller.signal, this.runtime.stop.signal]),
      failures: 0
    }
    this.readers.set(reader.id, self)
    this.workers.set(
      reader.id,
      this.runReader(reader, self).finally(() => {
        this.workers.delete(reader.id)
        this.readers.delete(reader.id)
        const index = state.readers.indexOf(reader)
        if (index >= 0) state.readers.splice(index, 1)
        this.host.scheduleUpdate()
      })
    )
  }

  private networkOf(reader: TelegramReaderState): TelegramDownloadNetwork | undefined {
    const { state } = this.runtime
    const network =
      state.networks.find((entry) => entry.id === reader.interfaceId && entry.enabled) ??
      state.networks.find((entry) => entry.enabled)
    return network
  }

  private async runReader(reader: TelegramReaderState, self: ReaderRuntime): Promise<void> {
    const { state, blocks } = this.runtime
    while (state.status === 'downloading' && !self.controller.signal.aborted) {
      if (!this.networkOf(reader)) {
        // No network is enabled right now: the manager pauses the download for this (see
        // reconcile), and the reader waits rather than spins.
        reader.status = 'pending'
        await delay(250, self.wait)
        continue
      }
      const block = blocks.find((entry) => entry.status === 'pending')
      if (!block) {
        if (blocks.some((entry) => entry.status === 'downloading')) {
          // Stay available: a block in flight may be handed back by its reader failing.
          reader.status = 'pending'
          await delay(250, self.wait)
          continue
        }
        reader.status = 'completed'
        break
      }
      // Claimed synchronously: no other reader can take it between the find and here.
      block.status = 'downloading'
      reader.interfaceId = this.networkOf(reader)!.id
      block.interfaceId = reader.interfaceId
      reader.currentBlockIndex = block.index
      reader.status = 'downloading'
      this.host.scheduleUpdate()

      const outcome = await this.runBlock(reader, self, block)
      const statusAfter = state.status as DownloadStatus
      if (outcome.type === 'completed') {
        self.failures = 0
        continue
      }
      this.handBack(block)
      if (outcome.type === 'stopped') {
        reader.status = statusAfter === 'paused' ? 'paused' : 'cancelled'
        break
      }
      if (outcome.type === 'retry') {
        reader.status = 'retrying'
        this.host.scheduleUpdate()
        await delay(outcome.waitMs, self.wait)
        if ((this.runtime.state.status as DownloadStatus) !== 'downloading') {
          reader.status =
            (this.runtime.state.status as DownloadStatus) === 'paused' ? 'paused' : 'cancelled'
          break
        }
        continue
      }
      // give-up: the reader is done, and the download fails with it.
      this.host.failDownload(describeError(outcome.error))
      reader.status = 'cancelled'
      break
    }
  }

  /** Returns a block to the pool, keeping what was written of it. */
  private handBack(block: HttpBlockState): void {
    if (block.status === 'downloading') block.status = 'pending'
  }

  private async runBlock(
    reader: TelegramReaderState,
    self: ReaderRuntime,
    block: HttpBlockState
  ): Promise<BlockOutcome> {
    const { state, meters } = this.runtime
    // The source (message lookup, its location) is found on first use; failing that, the
    // download ends with why.
    let source: TelegramFileSource
    try {
      source = await this.source()
    } catch (cause) {
      return {
        type: 'give-up',
        error:
          cause instanceof TelegramFileError
            ? cause
            : new TelegramFileError('missing', describeError(cause))
      }
    }
    const networkId = reader.interfaceId
    const openEnded = block.rangeEnd === null
    const end = block.rangeEnd === null ? Number.MAX_SAFE_INTEGER : block.rangeEnd + 1
    let offset = block.rangeStart + block.bytesDownloaded

    while (offset < end) {
      if (self.controller.signal.aborted || (state.status as string) !== 'downloading') {
        return { type: 'stopped' }
      }
      const remaining = end - offset
      let request = Math.min(FILE_CHUNK_BYTES, remaining)
      const aligned = Math.floor(request / 4096) * 4096
      // Mid-file the answer is exactly the limit, which must be 4096-divisible; a tail smaller
      // than that over-asks and slices the answer down.
      request = aligned > 0 ? aligned : 4096

      let buffer: Buffer
      try {
        buffer = await source.request(offset, request, self.controller.signal)
      } catch (error) {
        return this.asOutcome(self, error)
      }
      if (buffer.length === 0) {
        if (!openEnded) {
          return {
            type: 'give-up',
            error: new Error('Telegram closed this file early — resume to try again.')
          }
        }
        // The end of the photo: its size is known now, and the block with it.
        state.totalBytes = offset
        advanceBlock(block, networkId, offset - block.rangeStart)
        block.status = 'completed'
        recomputeAggregates(state, this.runtime.blocks)
        this.host.scheduleUpdate()
        return { type: 'completed' }
      }

      const usable = Math.min(buffer.length, remaining)
      try {
        await this.host.limits.take(networkId, usable)
        await this.write(offset, buffer.subarray(0, usable))
      } catch (error) {
        // The write side (a full disk, a dropped drive) isn't Telegram's doing: end the download
        // with what it says, keeping the download resumable.
        this.handBack(block)
        this.host.failDownload(describeError(error))
        return { type: 'stopped' }
      }

      const gained = advanceBlock(block, networkId, offset + usable - block.rangeStart)
      if (gained > 0) {
        state.bytesDownloaded += gained
        reader.bytesDownloaded += gained
        meters.add(reader.id, networkId, gained)
      }
      offset += usable
      this.host.scheduleUpdate()
    }

    if (block.rangeEnd !== null) {
      advanceBlock(block, networkId, block.rangeEnd - block.rangeStart + 1)
    }
    block.status = 'completed'
    recomputeAggregates(state, this.runtime.blocks)
    this.host.scheduleUpdate()
    return { type: 'completed' }
  }

  /** Reads failure kinds the transfer acts on; 'rate' becomes a wait, the rest retries or end
   * the download. */
  private asOutcome(self: ReaderRuntime, error: unknown): BlockOutcome {
    if (error instanceof TelegramFileError) {
      if (error.kind === 'rate') {
        const waitMs = (error.waitSeconds ?? 1) * 1000
        if (waitMs > RATE_WAIT_MAX_MS) return { type: 'give-up', error }
        return { type: 'retry', waitMs }
      }
      if (error.kind === 'auth' || error.kind === 'missing') {
        return { type: 'give-up', error }
      }
    }
    // A connection or server failure: back off and take the block again. Progress made resets
    // the count, as the HTTP transfer's does.
    self.failures += 1
    if (self.failures > MAX_RETRIES) return { type: 'give-up', error }
    const exact = Math.min(RETRY_BASE_MS * 2 ** (self.failures - 1), RETRY_MAX_MS)
    return { type: 'retry', waitMs: exact * (0.8 + 0.4 * Math.random()) }
  }

  private write(offset: number, buffer: Buffer): Promise<void> {
    return new Promise((resolve, reject) => {
      const writer = this.runtime.file.writer(offset)
      writer.on('error', reject)
      writer.end(buffer, () => {
        writer.off('error', reject)
        resolve()
      })
    })
  }
}

/** A download of a photo, whose byte size Telegram doesn't say ahead of time: one reader runs
 * it to the end, and the size is known once the last chunk lands. */
export function photoDownload(request: StartTelegramDownloadRequest): boolean {
  return request.mediaType === 'photo'
}
