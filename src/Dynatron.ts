import DynamoDB, {
  CreateTableInput,
  DocumentClient,
  UpdateTableInput,
  UpdateTimeToLiveInput,
} from "aws-sdk/clients/dynamodb";

import { DynatronConstructorParameters as DynatronConstructorParameters } from "../types/request";
import { BatchDeleter } from "./requesters/batch-deleter";
import { BatchGetter } from "./requesters/batch-getter";
import { BatchPutter } from "./requesters/batch-putter";
import { Checker } from "./requesters/checker";
import { Deleter } from "./requesters/deleter";
import { Getter } from "./requesters/getter";
import { Putter } from "./requesters/putter";
import { Querier } from "./requesters/querier";
import { Scanner } from "./requesters/scanner";
import { TableCreator } from "./requesters/tables/table-creator";
import { TableDeleter } from "./requesters/tables/table-deleter";
import { TableDescriber } from "./requesters/tables/table-describer";
import { TableTTLDescriber } from "./requesters/tables/table-ttl-describer";
import { TableTTLUpdater } from "./requesters/tables/table-ttl-updater";
import { TableUpdater } from "./requesters/tables/table-updater";
import { TablesLister } from "./requesters/tables/tables-lister";
import { TransactGetter } from "./requesters/transact-getter";
import { TransactWriter } from "./requesters/transact-writer";
import { Updater } from "./requesters/updater";
import { initDB, initDocumentClient } from "./utils/misc-utils";

export class Dynatron {
  protected static readonly DynamoDBs: Record<string, DynamoDB> = {};
  protected static readonly DocumentClients: Record<
    string,
    DocumentClient
  > = {};

  constructor(
    private readonly parameters: DynatronConstructorParameters,
    private instanceId = "default",
  ) {
    Dynatron.DynamoDBs[this.instanceId] =
      Dynatron.DynamoDBs[this.instanceId] || initDB(parameters.clientConfigs);
    Dynatron.DocumentClients[this.instanceId] =
      Dynatron.DocumentClients[this.instanceId] ||
      initDocumentClient(parameters.clientConfigs);
  }

  batchDelete = (keys: DocumentClient.Key[]) =>
    new BatchDeleter(
      Dynatron.DocumentClients[this.instanceId],
      this.parameters.table,
      keys,
    );

  batchGet = (keys: DocumentClient.Key[]) =>
    new BatchGetter(
      Dynatron.DocumentClients[this.instanceId],
      this.parameters.table,
      keys,
    );

  batchPut = (items: DocumentClient.PutItemInputAttributeMap[]) =>
    new BatchPutter(
      Dynatron.DocumentClients[this.instanceId],
      this.parameters.table,
      items,
    );

  check = (key: DocumentClient.Key) =>
    new Checker(
      Dynatron.DocumentClients[this.instanceId],
      this.parameters.table,
      key,
    );

  delete = (key: DocumentClient.Key) =>
    new Deleter(
      Dynatron.DocumentClients[this.instanceId],
      this.parameters.table,
      key,
    );

  get = (key: DocumentClient.Key) =>
    new Getter(
      Dynatron.DocumentClients[this.instanceId],
      this.parameters.table,
      key,
    );

  put = (item: DocumentClient.PutItemInputAttributeMap) =>
    new Putter(
      Dynatron.DocumentClients[this.instanceId],
      this.parameters.table,
      item,
    );

  query = (...arguments_: [DocumentClient.Key] | [string, any]) =>
    new Querier(
      Dynatron.DocumentClients[this.instanceId],
      this.parameters.table,
      typeof arguments_[0] === "string"
        ? { [arguments_[0]]: arguments_[1] }
        : arguments_[0],
    );

  scan = () =>
    new Scanner(
      Dynatron.DocumentClients[this.instanceId],
      this.parameters.table,
    );

  update = (key: DocumentClient.Key) =>
    new Updater(
      Dynatron.DocumentClients[this.instanceId],
      this.parameters.table,
      key,
    );

  transactGet = (items: Getter[]) =>
    new TransactGetter(
      Dynatron.DocumentClients[this.instanceId],
      this.parameters.table,
      items,
    );

  transactWrite = (items: (Checker | Putter | Deleter | Updater)[]) =>
    new TransactWriter(
      Dynatron.DocumentClients[this.instanceId],
      this.parameters.table,
      items,
    );

  public get Tables() {
    return {
      create: (parameters: CreateTableInput) =>
        new TableCreator(Dynatron.DynamoDBs[this.instanceId], parameters),
      delete: (table: string) =>
        new TableDeleter(Dynatron.DynamoDBs[this.instanceId], table),
      describe: (table: string) =>
        new TableDescriber(Dynatron.DynamoDBs[this.instanceId], table),
      describeTTL: (table: string) =>
        new TableTTLDescriber(Dynatron.DynamoDBs[this.instanceId], table),
      list: () => new TablesLister(Dynatron.DynamoDBs[this.instanceId]),
      update: (parameters: UpdateTableInput) =>
        new TableUpdater(Dynatron.DynamoDBs[this.instanceId], parameters),
      updateTTL: (parameters: UpdateTimeToLiveInput) =>
        new TableTTLUpdater(Dynatron.DynamoDBs[this.instanceId], parameters),
    };
  }
}
