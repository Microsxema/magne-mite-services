import {createClient} from "redis";
import {Log} from "@magne-mite/types/types";
import {IWeightedKeysListsRepository} from "@magne-mite/types/interfaces";

type RedisClientType = ReturnType<typeof createClient>;

export class EventLogsRedisRepository implements IWeightedKeysListsRepository<Log> {

  protected get indexName()       { return this.network + '.Logs.Range' }
  protected get prefixListName()  { return this.network + ':logs:'      }
  protected get allKeysQuery()    { return this.prefixListName + "*"    }

  private blockNumberKeyToKeyWithPrefix = (blockNumberKey: string) => this.prefixListName + blockNumberKey;
  private keyWithPrefixToBlockNumberKey = (keyWithPrefix: string) => keyWithPrefix.replace(this.prefixListName, '');

  constructor(
    protected readonly network: string,
    protected readonly redisClient: RedisClientType,
  ) {
  }

  public async push(blockNumberKey: string, ...log: Log[]): Promise<number> {
    if (log.length === 0 ) {
      await this.redisClient.zAdd(this.indexName, {
        value: this.blockNumberKeyToKeyWithPrefix(blockNumberKey),
        score: parseInt(blockNumberKey),
      });

      return 0;
    }

    const encodedLogs = log
      .map(log => JSON.stringify(log))

    const [logsLen, ] = await Promise.all([
      this.redisClient.lPush(this.blockNumberKeyToKeyWithPrefix(blockNumberKey), encodedLogs),
      this.redisClient.zAdd(this.indexName, {
        value: this.blockNumberKeyToKeyWithPrefix(blockNumberKey),
        score: parseInt(blockNumberKey),
      }),
    ]);

    return logsLen;
  }

  /** Getters keys Area */
  private async _getListKeysWithPrefix(): Promise<string[]> {
    return await this.redisClient.keys(
      this.allKeysQuery
    )
  }

  private async _getKeysWithPrefixOfListsByScore(min: number, max: number): Promise<string[]> {
    return await this.redisClient.zRangeByScore(
      this.indexName,
      min, max,
    )
  }

  private async _getKeysWithPrefixOfListsByPosition(start: number, stop: number): Promise<string[]> {
    return await this.redisClient.zRange(
      this.indexName,
      start, stop,
    )
  }

  public async getListKeys(): Promise<string[]> {
    return (await this._getListKeysWithPrefix())
      .map(k => this.keyWithPrefixToBlockNumberKey(k))
  }

  public async getListKeysCount(): Promise<number> {
    return await this.redisClient.zCount(
      this.indexName,
      '-inf', '+inf',
    );
  }

  public async getKeysOfListsByScore(min: number, max: number): Promise<string[]> {
    return (await this._getKeysWithPrefixOfListsByScore(min, max))
      .map(k => this.keyWithPrefixToBlockNumberKey(k))
  }

  public async getKeysOfListsByPosition(start: number, stop: number): Promise<string[]> {
    return (await this._getKeysWithPrefixOfListsByPosition(start, stop))
      .map(k => this.keyWithPrefixToBlockNumberKey(k))
  }

  /** Getters list Area */
  public async getMergedValuesOfListsByScore(min: number, max: number): Promise<Log[]> {
    return (await this.redisClient.mGet(
      await this._getKeysWithPrefixOfListsByScore(min, min)
    ))
      .filter(logs => logs)
      .flat()
      .map(log => JSON.parse(log))
  }

  public async getListValues(blockNumberKey: string, start: number, stop: number): Promise<Log[]> {
    return (await this.redisClient.lRange(
      this.blockNumberKeyToKeyWithPrefix(blockNumberKey),
      start, stop,
    )).map(value => JSON.parse(value))
  }

  /** Remove Area */
  private async _removeList(keyWithPrefix: string) {
    await Promise.all([
      this.redisClient.del(keyWithPrefix),
      this.redisClient.zRem(this.indexName, keyWithPrefix),
    ]);
  }

  private async _removeLists(keysWithPrefix: string[]) {
    await Promise.all(
      keysWithPrefix.map(async k =>
        await this._removeList(k)
      )
    )
  }

  public async removeList(blockNumberKey: string | string[]): Promise<void> {
    if (blockNumberKey instanceof Array) {
      await this._removeLists(
        blockNumberKey.map(bnk => this.blockNumberKeyToKeyWithPrefix(bnk))
      )
    } else {
      await this._removeList(
        this.blockNumberKeyToKeyWithPrefix(blockNumberKey)
      )
    }
  }

  public async removeListsByScore(min: number, max: number): Promise<void> {
    await this._removeLists(
      await this._getKeysWithPrefixOfListsByScore(min, max),
    )
  }

  public async removeListsByPosition(start: number, stop: number): Promise<void> {
    await this._removeLists(
      await this._getKeysWithPrefixOfListsByPosition(start, stop),
    )
  }
}
