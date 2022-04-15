import Denque = require('denque');
import type { Readable } from 'stream';

import type { Document, Timestamp } from './bson';
import { Collection } from './collection';
import {
  AbstractCursor,
  AbstractCursorEvents,
  AbstractCursorOptions,
  CursorStreamOptions
} from './cursor/abstract_cursor';
import { Db } from './db';
import {
  AnyError,
  isResumableError,
  MongoAPIError,
  MongoChangeStreamError,
  MongoError,
  MongoRuntimeError
} from './error';
import { MongoClient } from './mongo_client';
import { InferIdType, Nullable, TypedEventEmitter } from './mongo_types';
import { AggregateOperation, AggregateOptions } from './operations/aggregate';
import type { CollationOptions, OperationParent } from './operations/command';
import { executeOperation, ExecutionResult } from './operations/execute_operation';
import type { ReadPreference } from './read_preference';
import type { Topology } from './sdam/topology';
import type { ClientSession, ServerSessionId } from './sessions';
import {
  calculateDurationInMs,
  Callback,
  getTopology,
  maxWireVersion,
  maybePromise,
  MongoDBNamespace,
  now
} from './utils';

/** @internal */
const kResumeQueue = Symbol('resumeQueue');
/** @internal */
const kCursorStream = Symbol('cursorStream');
/** @internal */
const kClosed = Symbol('closed');
/** @internal */
const kMode = Symbol('mode');

const CHANGE_STREAM_OPTIONS = [
  'resumeAfter',
  'startAfter',
  'startAtOperationTime',
  'fullDocument'
] as const;

const CURSOR_OPTIONS = [
  'batchSize',
  'maxAwaitTimeMS',
  'collation',
  'readPreference',
  'comment',
  ...CHANGE_STREAM_OPTIONS
] as const;

const CHANGE_DOMAIN_TYPES = {
  COLLECTION: Symbol('Collection'),
  DATABASE: Symbol('Database'),
  CLUSTER: Symbol('Cluster')
};

const NO_RESUME_TOKEN_ERROR =
  'A change stream document has been received that lacks a resume token (_id).';
const NO_CURSOR_ERROR = 'ChangeStream has no cursor';
const CHANGESTREAM_CLOSED_ERROR = 'ChangeStream is closed';

/** @public */
export interface ResumeOptions {
  startAtOperationTime?: Timestamp;
  batchSize?: number;
  maxAwaitTimeMS?: number;
  collation?: CollationOptions;
  readPreference?: ReadPreference;
  resumeAfter?: ResumeToken;
  startAfter?: ResumeToken;
}

/**
 * Represents the logical starting point for a new ChangeStream or resuming a ChangeStream on the server.
 * @see https://www.mongodb.com/docs/manual/changeStreams/#std-label-change-stream-resume
 * @public
 */
export type ResumeToken = unknown;

/**
 * Represents a specific point in time on a server. Can be retrieved by using `db.command()`
 * @public
 * @see https://docs.mongodb.com/manual/reference/method/db.runCommand/#response
 */
export type OperationTime = Timestamp;

/** @public */
export interface PipeOptions {
  end?: boolean;
}

/**
 * Options that can be passed to a ChangeStream. Note that startAfter, resumeAfter, and startAtOperationTime are all mutually exclusive, and the server will error if more than one is specified.
 * @public
 */
export interface ChangeStreamOptions extends AggregateOptions {
  /**
   * Allowed values: 'updateLookup'. When set to 'updateLookup',
   * the change stream will include both a delta describing the changes to the document,
   * as well as a copy of the entire document that was changed from some time after the change occurred.
   */
  fullDocument?: string;
  /** The maximum amount of time for the server to wait on new documents to satisfy a change stream query. */
  maxAwaitTimeMS?: number;
  /**
   * Allows you to start a changeStream after a specified event.
   * @see https://docs.mongodb.com/manual/changeStreams/#resumeafter-for-change-streams
   */
  resumeAfter?: ResumeToken;
  /**
   * Similar to resumeAfter, but will allow you to start after an invalidated event.
   * @see https://docs.mongodb.com/manual/changeStreams/#startafter-for-change-streams
   */
  startAfter?: ResumeToken;
  /** Will start the changeStream after the specified operationTime. */
  startAtOperationTime?: OperationTime;
  /**
   * The number of documents to return per batch.
   * @see https://docs.mongodb.com/manual/reference/command/aggregate
   */
  batchSize?: number;
}

/** @public */
export interface ChangeStreamNameSpace {
  db: string;
  coll: string;
}

/** @public */
export interface ChangeStreamDocumentKey<TSchema extends Document = Document> {
  /**
   * For unsharded collections this contains a single field `_id`.
   * For sharded collections, this will contain all the components of the shard key
   */
  documentKey: { _id: InferIdType<TSchema>; [shardKey: string]: any };
}

/** @public */
export interface ChangeStreamDocumentCommon {
  /**
   * The id functions as an opaque token for use when resuming an interrupted
   * change stream.
   */
  _id: ResumeToken;
  /**
   * The timestamp from the oplog entry associated with the event.
   * For events that happened as part of a multi-document transaction, the associated change stream
   * notifications will have the same clusterTime value, namely the time when the transaction was committed.
   * On a sharded cluster, events that occur on different shards can have the same clusterTime but be
   * associated with different transactions or even not be associated with any transaction.
   * To identify events for a single transaction, you can use the combination of lsid and txnNumber in the change stream event document.
   */
  clusterTime?: Timestamp;

  /**
   * The transaction number.
   * Only present if the operation is part of a multi-document transaction.
   *
   * **NOTE:** txnNumber can be a Long if promoteLongs is set to false
   */
  txnNumber?: number;

  /**
   * The identifier for the session associated with the transaction.
   * Only present if the operation is part of a multi-document transaction.
   */
  lsid?: ServerSessionId;
}

/**
 * @public
 * @see https://www.mongodb.com/docs/manual/reference/change-events/#insert-event
 */
export interface ChangeStreamInsertDocument<TSchema extends Document = Document>
  extends ChangeStreamDocumentCommon,
    ChangeStreamDocumentKey<TSchema> {
  /** Describes the type of operation represented in this change notification */
  operationType: 'insert';
  /** This key will contain the document being inserted */
  fullDocument: TSchema;
  /** Namespace the insert event occured on */
  ns: ChangeStreamNameSpace;
}

/**
 * @public
 * @see https://www.mongodb.com/docs/manual/reference/change-events/#update-event
 */
export interface ChangeStreamUpdateDocument<TSchema extends Document = Document>
  extends ChangeStreamDocumentCommon,
    ChangeStreamDocumentKey<TSchema> {
  /** Describes the type of operation represented in this change notification */
  operationType: 'update';
  /**
   * This is only set if `fullDocument` is set to `'updateLookup'`
   * The fullDocument document represents the most current majority-committed version of the updated document.
   * The fullDocument document may vary from the document at the time of the update operation depending on the
   * number of interleaving majority-committed operations that occur between the update operation and the document lookup.
   */
  fullDocument?: TSchema;
  /** Contains a description of updated and removed fields in this operation */
  updateDescription: UpdateDescription<TSchema>;
  /** Namespace the update event occured on */
  ns: ChangeStreamNameSpace;
}

/**
 * @public
 * @see https://www.mongodb.com/docs/manual/reference/change-events/#replace-event
 */
export interface ChangeStreamReplaceDocument<TSchema extends Document = Document>
  extends ChangeStreamDocumentCommon,
    ChangeStreamDocumentKey<TSchema> {
  /** Describes the type of operation represented in this change notification */
  operationType: 'replace';
  /** The fullDocument of a replace event represents the document after the insert of the replacement document */
  fullDocument: TSchema;
  /** Namespace the replace event occured on */
  ns: ChangeStreamNameSpace;
}

/**
 * @public
 * @see https://www.mongodb.com/docs/manual/reference/change-events/#delete-event
 */
export interface ChangeStreamDeleteDocument<TSchema extends Document = Document>
  extends ChangeStreamDocumentCommon,
    ChangeStreamDocumentKey<TSchema> {
  /** Describes the type of operation represented in this change notification */
  operationType: 'delete';
  /** Namespace the delete event occured on */
  ns: ChangeStreamNameSpace;
}

/**
 * @public
 * @see https://www.mongodb.com/docs/manual/reference/change-events/#drop-event
 */
export interface ChangeStreamDropDocument extends ChangeStreamDocumentCommon {
  /** Describes the type of operation represented in this change notification */
  operationType: 'drop';
  /** Namespace the drop event occured on */
  ns: ChangeStreamNameSpace;
}

/**
 * @public
 * @see https://www.mongodb.com/docs/manual/reference/change-events/#rename-event
 */
export interface ChangeStreamRenameDocument extends ChangeStreamDocumentCommon {
  /** Describes the type of operation represented in this change notification */
  operationType: 'rename';
  /** The new name for the `ns.coll` collection */
  to: { db: string; coll: string };
  /** The "from" namespace that the rename occured on */
  ns: ChangeStreamNameSpace;
}

/**
 * @public
 * @see https://www.mongodb.com/docs/manual/reference/change-events/#dropdatabase-event
 */
export interface ChangeStreamDropDatabaseDocument extends ChangeStreamDocumentCommon {
  /** Describes the type of operation represented in this change notification */
  operationType: 'dropDatabase';
  /** The database dropped */
  ns: { db: string };
}

/**
 * @public
 * @see https://www.mongodb.com/docs/manual/reference/change-events/#invalidate-event
 */
export interface ChangeStreamInvalidateDocument extends ChangeStreamDocumentCommon {
  /** Describes the type of operation represented in this change notification */
  operationType: 'invalidate';
}

/** @public */
export type ChangeStreamDocument<TSchema extends Document = Document> =
  | ChangeStreamInsertDocument<TSchema>
  | ChangeStreamUpdateDocument<TSchema>
  | ChangeStreamReplaceDocument<TSchema>
  | ChangeStreamDeleteDocument<TSchema>
  | ChangeStreamDropDocument
  | ChangeStreamRenameDocument
  | ChangeStreamDropDatabaseDocument
  | ChangeStreamInvalidateDocument;

/** @public */
export interface UpdateDescription<TSchema extends Document = Document> {
  /**
   * A document containing key:value pairs of names of the fields that were
   * changed, and the new value for those fields.
   */
  updatedFields?: Partial<TSchema>;

  /**
   * An array of field names that were removed from the document.
   */
  removedFields?: string[];

  /**
   * An array of documents which record array truncations performed with pipeline-based updates using one or more of the following stages:
   * - $addFields
   * - $set
   * - $replaceRoot
   * - $replaceWith
   */
  truncatedArrays?: Array<{
    /** The name of the truncated field. */
    field: string;
    /** The number of elements in the truncated array. */
    newSize: number;
  }>;
}

/** @public */
export type ChangeStreamEvents<TSchema extends Document = Document> = {
  resumeTokenChanged(token: ResumeToken): void;
  init(response: TSchema): void;
  more(response?: TSchema | undefined): void;
  response(): void;
  end(): void;
  error(error: Error): void;
  change(change: ChangeStreamDocument<TSchema>): void;
} & AbstractCursorEvents;

/**
 * Creates a new Change Stream instance. Normally created using {@link Collection#watch|Collection.watch()}.
 * @public
 */
export class ChangeStream<TSchema extends Document = Document> extends TypedEventEmitter<
  ChangeStreamEvents<TSchema>
> {
  pipeline: Document[];
  options: ChangeStreamOptions;
  parent: MongoClient | Db | Collection;
  namespace: MongoDBNamespace;
  type: symbol;
  /** @internal */
  cursor?: ChangeStreamCursor<TSchema>;
  streamOptions?: CursorStreamOptions;
  /** @internal */
  [kResumeQueue]: Denque<Callback<ChangeStreamCursor<TSchema>>>;
  /** @internal */
  [kCursorStream]?: Readable;
  /** @internal */
  [kClosed]: boolean;
  /** @internal */
  [kMode]: false | 'iterator' | 'emitter';

  /** @event */
  static readonly RESPONSE = 'response' as const;
  /** @event */
  static readonly MORE = 'more' as const;
  /** @event */
  static readonly INIT = 'init' as const;
  /** @event */
  static readonly CLOSE = 'close' as const;
  /**
   * Fired for each new matching change in the specified namespace. Attaching a `change`
   * event listener to a Change Stream will switch the stream into flowing mode. Data will
   * then be passed as soon as it is available.
   * @event
   */
  static readonly CHANGE = 'change' as const;
  /** @event */
  static readonly END = 'end' as const;
  /** @event */
  static readonly ERROR = 'error' as const;
  /**
   * Emitted each time the change stream stores a new resume token.
   * @event
   */
  static readonly RESUME_TOKEN_CHANGED = 'resumeTokenChanged' as const;

  /**
   * @internal
   *
   * @param parent - The parent object that created this change stream
   * @param pipeline - An array of {@link https://docs.mongodb.com/manual/reference/operator/aggregation-pipeline/|aggregation pipeline stages} through which to pass change stream documents
   */
  constructor(
    parent: OperationParent,
    pipeline: Document[] = [],
    options: ChangeStreamOptions = {}
  ) {
    super();

    this.pipeline = pipeline;
    this.options = options;

    if (parent instanceof Collection) {
      this.type = CHANGE_DOMAIN_TYPES.COLLECTION;
    } else if (parent instanceof Db) {
      this.type = CHANGE_DOMAIN_TYPES.DATABASE;
    } else if (parent instanceof MongoClient) {
      this.type = CHANGE_DOMAIN_TYPES.CLUSTER;
    } else {
      throw new MongoChangeStreamError(
        'Parent provided to ChangeStream constructor must be an instance of Collection, Db, or MongoClient'
      );
    }

    this.parent = parent;
    this.namespace = parent.s.namespace;
    if (!this.options.readPreference && parent.readPreference) {
      this.options.readPreference = parent.readPreference;
    }

    this[kResumeQueue] = new Denque();

    // Create contained Change Stream cursor
    this.cursor = createChangeStreamCursor(this, options);

    this[kClosed] = false;
    this[kMode] = false;

    // Listen for any `change` listeners being added to ChangeStream
    this.on('newListener', eventName => {
      if (eventName === 'change' && this.cursor && this.listenerCount('change') === 0) {
        streamEvents(this, this.cursor);
      }
    });

    this.on('removeListener', eventName => {
      if (eventName === 'change' && this.listenerCount('change') === 0 && this.cursor) {
        this[kCursorStream]?.removeAllListeners('data');
      }
    });
  }

  /** @internal */
  get cursorStream(): Readable | undefined {
    return this[kCursorStream];
  }

  /** The cached resume token that is used to resume after the most recently returned change. */
  get resumeToken(): ResumeToken {
    return this.cursor?.resumeToken;
  }

  /** Check if there is any document still available in the Change Stream */
  hasNext(): Promise<boolean>;
  hasNext(callback: Callback<boolean>): void;
  hasNext(callback?: Callback): Promise<boolean> | void {
    setIsIterator(this);
    return maybePromise(callback, cb => {
      getCursor(this, (err, cursor) => {
        if (err || !cursor) return cb(err); // failed to resume, raise an error
        cursor.hasNext(cb);
      });
    });
  }

  /** Get the next available document from the Change Stream. */
  next(): Promise<ChangeStreamDocument<TSchema>>;
  next(callback: Callback<ChangeStreamDocument<TSchema>>): void;
  next(
    callback?: Callback<ChangeStreamDocument<TSchema>>
  ): Promise<ChangeStreamDocument<TSchema>> | void {
    setIsIterator(this);
    return maybePromise(callback, cb => {
      getCursor(this, (err, cursor) => {
        if (err || !cursor) return cb(err); // failed to resume, raise an error
        cursor.next((error, change) => {
          if (error) {
            this[kResumeQueue].push(() => this.next(cb));
            processError(this, error, cb);
            return;
          }
          processNewChange<TSchema>(this, change, cb);
        });
      });
    });
  }

  /** Is the cursor closed */
  get closed(): boolean {
    return this[kClosed] || (this.cursor?.closed ?? false);
  }

  /** Close the Change Stream */
  close(callback?: Callback): Promise<void> | void {
    this[kClosed] = true;

    return maybePromise(callback, cb => {
      if (!this.cursor) {
        return cb();
      }

      const cursor = this.cursor;
      return cursor.close(err => {
        endStream(this);
        this.cursor = undefined;
        return cb(err);
      });
    });
  }

  /**
   * Return a modified Readable stream including a possible transform method.
   * @throws MongoDriverError if this.cursor is undefined
   */
  stream(options?: CursorStreamOptions): Readable {
    this.streamOptions = options;
    if (!this.cursor) throw new MongoChangeStreamError(NO_CURSOR_ERROR);
    return this.cursor.stream(options);
  }

  /**
   * Try to get the next available document from the Change Stream's cursor or `null` if an empty batch is returned
   */
  tryNext(): Promise<Document | null>;
  tryNext(callback: Callback<Document | null>): void;
  tryNext(callback?: Callback<Document | null>): Promise<Document | null> | void {
    setIsIterator(this);
    return maybePromise(callback, cb => {
      getCursor(this, (err, cursor) => {
        if (err || !cursor) return cb(err); // failed to resume, raise an error
        return cursor.tryNext(cb);
      });
    });
  }
}

/** @internal */
export interface ChangeStreamCursorOptions extends AbstractCursorOptions {
  startAtOperationTime?: OperationTime;
  resumeAfter?: ResumeToken;
  startAfter?: ResumeToken;
}

/** @internal */
export class ChangeStreamCursor<TSchema extends Document = Document> extends AbstractCursor<
  ChangeStreamDocument<TSchema>,
  ChangeStreamEvents
> {
  _resumeToken: ResumeToken;
  startAtOperationTime?: OperationTime;
  hasReceived?: boolean;
  resumeAfter: ResumeToken;
  startAfter: ResumeToken;
  options: ChangeStreamCursorOptions;

  postBatchResumeToken?: ResumeToken;
  pipeline: Document[];

  constructor(
    topology: Topology,
    namespace: MongoDBNamespace,
    pipeline: Document[] = [],
    options: ChangeStreamCursorOptions = {}
  ) {
    super(topology, namespace, options);

    this.pipeline = pipeline;
    this.options = options;
    this._resumeToken = null;
    this.startAtOperationTime = options.startAtOperationTime;

    if (options.startAfter) {
      this.resumeToken = options.startAfter;
    } else if (options.resumeAfter) {
      this.resumeToken = options.resumeAfter;
    }
  }

  set resumeToken(token: ResumeToken) {
    this._resumeToken = token;
    this.emit(ChangeStream.RESUME_TOKEN_CHANGED, token);
  }

  get resumeToken(): ResumeToken {
    return this._resumeToken;
  }

  get resumeOptions(): ResumeOptions {
    const result: ResumeOptions = applyKnownOptions(this.options, CURSOR_OPTIONS);

    if (this.resumeToken || this.startAtOperationTime) {
      for (const key of ['resumeAfter', 'startAfter', 'startAtOperationTime']) {
        Reflect.deleteProperty(result, key);
      }

      if (this.resumeToken) {
        const resumeKey =
          this.options.startAfter && !this.hasReceived ? 'startAfter' : 'resumeAfter';

        result[resumeKey] = this.resumeToken;
      } else if (this.startAtOperationTime && maxWireVersion(this.server) >= 7) {
        result.startAtOperationTime = this.startAtOperationTime;
      }
    }

    return result;
  }

  cacheResumeToken(resumeToken: ResumeToken): void {
    if (this.bufferedCount() === 0 && this.postBatchResumeToken) {
      this.resumeToken = this.postBatchResumeToken;
    } else {
      this.resumeToken = resumeToken;
    }
    this.hasReceived = true;
  }

  _processBatch(batchName: string, response?: Document): void {
    const cursor = response?.cursor || {};
    if (cursor.postBatchResumeToken) {
      this.postBatchResumeToken = cursor.postBatchResumeToken;

      if (cursor[batchName].length === 0) {
        this.resumeToken = cursor.postBatchResumeToken;
      }
    }
  }

  clone(): AbstractCursor<ChangeStreamDocument<TSchema>> {
    return new ChangeStreamCursor(this.topology, this.namespace, this.pipeline, {
      ...this.cursorOptions
    });
  }

  _initialize(session: ClientSession, callback: Callback<ExecutionResult>): void {
    const aggregateOperation = new AggregateOperation(this.namespace, this.pipeline, {
      ...this.cursorOptions,
      ...this.options,
      session
    });

    executeOperation(session, aggregateOperation, (err, response) => {
      if (err || response == null) {
        return callback(err);
      }

      const server = aggregateOperation.server;
      if (
        this.startAtOperationTime == null &&
        this.resumeAfter == null &&
        this.startAfter == null &&
        maxWireVersion(server) >= 7
      ) {
        this.startAtOperationTime = response.operationTime;
      }

      this._processBatch('firstBatch', response);

      this.emit(ChangeStream.INIT, response);
      this.emit(ChangeStream.RESPONSE);

      // TODO: NODE-2882
      callback(undefined, { server, session, response });
    });
  }

  override _getMore(batchSize: number, callback: Callback): void {
    super._getMore(batchSize, (err, response) => {
      if (err) {
        return callback(err);
      }

      this._processBatch('nextBatch', response);

      this.emit(ChangeStream.MORE, response);
      this.emit(ChangeStream.RESPONSE);
      callback(err, response);
    });
  }
}

const CHANGE_STREAM_EVENTS = [
  ChangeStream.RESUME_TOKEN_CHANGED,
  ChangeStream.END,
  ChangeStream.CLOSE
];

function setIsEmitter<TSchema extends Document>(changeStream: ChangeStream<TSchema>): void {
  if (changeStream[kMode] === 'iterator') {
    // TODO(NODE-3485): Replace with MongoChangeStreamModeError
    throw new MongoAPIError(
      'ChangeStream cannot be used as an EventEmitter after being used as an iterator'
    );
  }
  changeStream[kMode] = 'emitter';
}

function setIsIterator<TSchema extends Document>(changeStream: ChangeStream<TSchema>): void {
  if (changeStream[kMode] === 'emitter') {
    // TODO(NODE-3485): Replace with MongoChangeStreamModeError
    throw new MongoAPIError(
      'ChangeStream cannot be used as an iterator after being used as an EventEmitter'
    );
  }
  changeStream[kMode] = 'iterator';
}

/**
 * Create a new change stream cursor based on self's configuration
 * @internal
 */
function createChangeStreamCursor<TSchema extends Document>(
  changeStream: ChangeStream<TSchema>,
  options: ChangeStreamOptions | ResumeOptions
): ChangeStreamCursor<TSchema> {
  const changeStreamStageOptions = applyKnownOptions(options, CHANGE_STREAM_OPTIONS);
  if (changeStream.type === CHANGE_DOMAIN_TYPES.CLUSTER) {
    changeStreamStageOptions.allChangesForCluster = true;
  }
  const pipeline = [{ $changeStream: changeStreamStageOptions } as Document].concat(
    changeStream.pipeline
  );

  const cursorOptions: ChangeStreamCursorOptions = applyKnownOptions(options, CURSOR_OPTIONS);

  const changeStreamCursor = new ChangeStreamCursor<TSchema>(
    getTopology(changeStream.parent),
    changeStream.namespace,
    pipeline,
    cursorOptions
  );

  for (const event of CHANGE_STREAM_EVENTS) {
    changeStreamCursor.on(event, e => changeStream.emit(event, e));
  }

  if (changeStream.listenerCount(ChangeStream.CHANGE) > 0) {
    streamEvents(changeStream, changeStreamCursor);
  }

  return changeStreamCursor;
}

function applyKnownOptions(source: Document, options: ReadonlyArray<string>) {
  const result: Document = {};

  for (const option of options) {
    if (option in source) {
      result[option] = source[option];
    }
  }

  return result;
}
interface TopologyWaitOptions {
  start?: number;
  timeout?: number;
  readPreference?: ReadPreference;
}
// This method performs a basic server selection loop, satisfying the requirements of
// ChangeStream resumability until the new SDAM layer can be used.
const SELECTION_TIMEOUT = 30000;
function waitForTopologyConnected(
  topology: Topology,
  options: TopologyWaitOptions,
  callback: Callback
) {
  setTimeout(() => {
    if (options && options.start == null) {
      options.start = now();
    }

    const start = options.start || now();
    const timeout = options.timeout || SELECTION_TIMEOUT;
    if (topology.isConnected()) {
      return callback();
    }

    if (calculateDurationInMs(start) > timeout) {
      // TODO(NODE-3497): Replace with MongoNetworkTimeoutError
      return callback(new MongoRuntimeError('Timed out waiting for connection'));
    }

    waitForTopologyConnected(topology, options, callback);
  }, 500); // this is an arbitrary wait time to allow SDAM to transition
}

function closeWithError<TSchema extends Document>(
  changeStream: ChangeStream<TSchema>,
  error: AnyError,
  callback?: Callback
): void {
  if (!callback) {
    changeStream.emit(ChangeStream.ERROR, error);
  }

  changeStream.close(() => callback && callback(error));
}

function streamEvents<TSchema extends Document>(
  changeStream: ChangeStream<TSchema>,
  cursor: ChangeStreamCursor<TSchema>
): void {
  setIsEmitter(changeStream);
  const stream = changeStream[kCursorStream] || cursor.stream();
  changeStream[kCursorStream] = stream;
  stream.on('data', change => processNewChange(changeStream, change));
  stream.on('error', error => processError(changeStream, error));
}

function endStream<TSchema extends Document>(changeStream: ChangeStream<TSchema>): void {
  const cursorStream = changeStream[kCursorStream];
  if (cursorStream) {
    ['data', 'close', 'end', 'error'].forEach(event => cursorStream.removeAllListeners(event));
    cursorStream.destroy();
  }

  changeStream[kCursorStream] = undefined;
}

function processNewChange<TSchema extends Document>(
  changeStream: ChangeStream<TSchema>,
  change: Nullable<ChangeStreamDocument<TSchema>>,
  callback?: Callback<ChangeStreamDocument<TSchema>>
) {
  if (changeStream[kClosed]) {
    // TODO(NODE-3485): Replace with MongoChangeStreamClosedError
    if (callback) callback(new MongoAPIError(CHANGESTREAM_CLOSED_ERROR));
    return;
  }

  // a null change means the cursor has been notified, implicitly closing the change stream
  if (change == null) {
    // TODO(NODE-3485): Replace with MongoChangeStreamClosedError
    return closeWithError(changeStream, new MongoRuntimeError(CHANGESTREAM_CLOSED_ERROR), callback);
  }

  if (change && !change._id) {
    return closeWithError(
      changeStream,
      new MongoChangeStreamError(NO_RESUME_TOKEN_ERROR),
      callback
    );
  }

  // cache the resume token
  changeStream.cursor?.cacheResumeToken(change._id);

  // wipe the startAtOperationTime if there was one so that there won't be a conflict
  // between resumeToken and startAtOperationTime if we need to reconnect the cursor
  changeStream.options.startAtOperationTime = undefined;

  // Return the change
  if (!callback) return changeStream.emit(ChangeStream.CHANGE, change);
  return callback(undefined, change);
}

function processError<TSchema extends Document>(
  changeStream: ChangeStream<TSchema>,
  error: AnyError,
  callback?: Callback
) {
  const cursor = changeStream.cursor;

  // If the change stream has been closed explicitly, do not process error.
  if (changeStream[kClosed]) {
    // TODO(NODE-3485): Replace with MongoChangeStreamClosedError
    if (callback) callback(new MongoAPIError(CHANGESTREAM_CLOSED_ERROR));
    return;
  }

  // if the resume succeeds, continue with the new cursor
  function resumeWithCursor(newCursor: ChangeStreamCursor<TSchema>) {
    changeStream.cursor = newCursor;
    processResumeQueue(changeStream);
  }

  // otherwise, raise an error and close the change stream
  function unresumableError(err: AnyError) {
    if (!callback) {
      changeStream.emit(ChangeStream.ERROR, err);
    }

    changeStream.close(() => processResumeQueue(changeStream, err));
  }

  if (cursor && isResumableError(error as MongoError, maxWireVersion(cursor.server))) {
    changeStream.cursor = undefined;

    // stop listening to all events from old cursor
    endStream(changeStream);

    // close internal cursor, ignore errors
    cursor.close();

    const topology = getTopology(changeStream.parent);
    waitForTopologyConnected(topology, { readPreference: cursor.readPreference }, err => {
      // if the topology can't reconnect, close the stream
      if (err) return unresumableError(err);

      // create a new cursor, preserving the old cursor's options
      const newCursor = createChangeStreamCursor(changeStream, cursor.resumeOptions);

      // attempt to continue in emitter mode
      if (!callback) return resumeWithCursor(newCursor);

      // attempt to continue in iterator mode
      newCursor.hasNext(err => {
        // if there's an error immediately after resuming, close the stream
        if (err) return unresumableError(err);
        resumeWithCursor(newCursor);
      });
    });
    return;
  }

  // if initial error wasn't resumable, raise an error and close the change stream
  return closeWithError(changeStream, error, callback);
}

/**
 * Safely provides a cursor across resume attempts
 *
 * @param changeStream - the parent ChangeStream
 */
function getCursor<T extends Document>(
  changeStream: ChangeStream<T>,
  callback: Callback<ChangeStreamCursor<T>>
) {
  if (changeStream[kClosed]) {
    // TODO(NODE-3485): Replace with MongoChangeStreamClosedError
    callback(new MongoAPIError(CHANGESTREAM_CLOSED_ERROR));
    return;
  }

  // if a cursor exists and it is open, return it
  if (changeStream.cursor) {
    callback(undefined, changeStream.cursor);
    return;
  }

  // no cursor, queue callback until topology reconnects
  changeStream[kResumeQueue].push(callback);
}

/**
 * Drain the resume queue when a new has become available
 *
 * @param changeStream - the parent ChangeStream
 * @param err - error getting a new cursor
 */
function processResumeQueue<TSchema extends Document>(
  changeStream: ChangeStream<TSchema>,
  err?: Error
) {
  while (changeStream[kResumeQueue].length) {
    const request = changeStream[kResumeQueue].pop();
    if (!request) break; // Should never occur but TS can't use the length check in the while condition

    if (!err) {
      if (changeStream[kClosed]) {
        // TODO(NODE-3485): Replace with MongoChangeStreamClosedError
        request(new MongoAPIError(CHANGESTREAM_CLOSED_ERROR));
        return;
      }
      if (!changeStream.cursor) {
        request(new MongoChangeStreamError(NO_CURSOR_ERROR));
        return;
      }
    }
    request(err, changeStream.cursor);
  }
}
