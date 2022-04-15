import { expectError, expectType } from 'tsd';

import type {
  ChangeStreamDeleteDocument,
  ChangeStreamDocument,
  ChangeStreamDocumentCommon,
  ChangeStreamDocumentKey,
  ChangeStreamDropDatabaseDocument,
  ChangeStreamDropDocument,
  ChangeStreamInsertDocument,
  ChangeStreamInvalidateDocument,
  ChangeStreamNameSpace,
  ChangeStreamOptions,
  ChangeStreamRenameDocument,
  ChangeStreamReplaceDocument,
  ChangeStreamUpdateDocument,
  ResumeToken,
  ServerSessionId,
  Timestamp,
  UpdateDescription
} from '../../src';

declare const changeStreamOptions: ChangeStreamOptions;
type ChangeStreamOperationType =
  | 'insert'
  | 'update'
  | 'replace'
  | 'delete'
  | 'invalidate'
  | 'drop'
  | 'dropDatabase'
  | 'rename';

// The change stream spec says that we cannot throw an error for invalid values to `fullDocument`
// for future compatibility.  This means we must leave `fullDocument` as type string.
expectType<string | undefined>(changeStreamOptions.fullDocument);

type Schema = { _id: number; a: number };
declare const change: ChangeStreamDocument<Schema>;

expectType<unknown>(change._id);
expectType<ChangeStreamOperationType>(change.operationType);

// The following are always defined ChangeStreamDocumentCommon
expectType<ChangeStreamDocument extends ChangeStreamDocumentCommon ? true : false>(true);
expectType<ResumeToken>(change._id);
expectType<Timestamp | undefined>(change.clusterTime);
expectType<number | undefined>(change.txnNumber); // Could be a Long if promoteLongs is off
expectType<ServerSessionId | undefined>(change.lsid);

// You must narrow to get to certain properties
expectError(change.fullDocument);

type CrudChangeDoc =
  | ChangeStreamInsertDocument<Schema> //  C
  | ChangeStreamReplaceDocument<Schema> // R
  | ChangeStreamUpdateDocument<Schema> //  U
  | ChangeStreamDeleteDocument<Schema>; // D
declare const crudChange: CrudChangeDoc;

// ChangeStreamDocumentKey
expectType<CrudChangeDoc extends ChangeStreamDocumentKey<Schema> ? true : false>(true);
expectType<number>(crudChange.documentKey._id); // _id will get typed
expectType<any>(crudChange.documentKey.blah); // shard keys could be anything

// ChangeStreamFullNameSpace
expectType<ChangeStreamNameSpace>(crudChange.ns);
expectType<{ db: string; coll: string }>(crudChange.ns);

switch (change.operationType) {
  case 'insert': {
    expectType<ChangeStreamInsertDocument<Schema>>(change);
    expectType<'insert'>(change.operationType);
    expectType<number>(change.documentKey._id);
    expectType<any>(change.documentKey.blah);
    expectType<Schema>(change.fullDocument);
    break;
  }
  case 'update': {
    expectType<ChangeStreamUpdateDocument<Schema>>(change);
    expectType<'update'>(change.operationType);
    expectType<Schema | undefined>(change.fullDocument); // Update only attaches fullDocument if configured
    expectType<UpdateDescription<Schema>>(change.updateDescription);
    expectType<Partial<Schema> | undefined>(change.updateDescription.updatedFields);
    expectType<string[] | undefined>(change.updateDescription.removedFields);
    expectType<Array<{ field: string; newSize: number }> | undefined>(
      change.updateDescription.truncatedArrays
    );
    break;
  }
  case 'replace': {
    expectType<ChangeStreamReplaceDocument<Schema>>(change);
    expectType<'replace'>(change.operationType);
    expectType<Schema>(change.fullDocument);
    break;
  }
  case 'delete': {
    expectType<ChangeStreamDeleteDocument<Schema>>(change);
    expectType<'delete'>(change.operationType);
    expectError(change.fullDocument); // Delete has no fullDocument
    break;
  }
  case 'drop': {
    expectType<ChangeStreamDropDocument>(change);
    expectType<'drop'>(change.operationType);
    expectType<{ db: string; coll: string }>(change.ns);
    break;
  }
  case 'rename': {
    expectType<ChangeStreamRenameDocument>(change);
    expectType<'rename'>(change.operationType);
    expectType<{ db: string; coll: string }>(change.ns);
    expectType<{ db: string; coll: string }>(change.to);
    break;
  }
  case 'dropDatabase': {
    expectType<ChangeStreamDropDatabaseDocument>(change);
    expectType<'dropDatabase'>(change.operationType);
    expectError(change.ns.coll);
    break;
  }
  case 'invalidate': {
    expectType<ChangeStreamInvalidateDocument>(change);
    expectType<'invalidate'>(change.operationType);
    expectError(change.ns);
    break;
  }
  default: {
    expectType<never>(change);
  }
}
