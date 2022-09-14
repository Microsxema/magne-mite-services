import Web3 from "web3";
import {
  Log,
  GetPastLogsPayload,
  BlockchainRepositoryOptions,
} from "@magne-mite/types/types";
import {
  IBlockchainRepository,
  IWeightedKeysListsRepository,
} from "@magne-mite/types/interfaces";

type GetLogsPayload = GetPastLogsPayload & {
  toBlockNumber: number
}

type PutLogsPayload = {
  logs: Log[],
  toBlockNumber: number
  fromBlockNumber: number,
}

type GetPastLogsFromNodeOptions = {
  filterByAddressesFlag: boolean
}

export class BlockchainRepositoryWithCaching implements IBlockchainRepository {
  constructor(
    protected readonly web3: Web3,
    protected readonly options: BlockchainRepositoryOptions,
    protected readonly logsRepository: IWeightedKeysListsRepository<Log>,
  ) {
  }

  protected async putLogsInRepository(payload: PutLogsPayload) {
    const pointersInLogsRepository = await this.getPointersInLogsRepository();

    const blocksRange = {
      fromBlockNumber: payload.fromBlockNumber,
      toBlockNumber: payload.fromBlockNumber,
    }

    const rangeLogsTable = new Map<number, Log[]>(
      Array
        .from({ length: blocksRange.toBlockNumber - pointersInLogsRepository.lastBlockNumber + 1 },
          (_, i) =>
            [i + pointersInLogsRepository.lastBlockNumber, []]
        )
    );

    payload.logs
      .filter(
        log =>
          log.blockNumber >= pointersInLogsRepository.lastBlockNumber
      )
      .forEach(
        log =>
          rangeLogsTable
            .get(log.blockNumber)
            .push(log)
      )

    if (
      pointersInLogsRepository.lastBlockNumber < blocksRange.fromBlockNumber &&
      (blocksRange.fromBlockNumber - pointersInLogsRepository.lastBlockNumber) !== 1
    ) {
      const uncollectedLogsResult = await this.getPastLogsFromNode({
        fromBlockNumber: pointersInLogsRepository.lastBlockNumber +1,
        toBlockNumber: blocksRange.fromBlockNumber -1,
      }, { filterByAddressesFlag: false });

      uncollectedLogsResult.logs.forEach(
        log =>
          rangeLogsTable
            .get(log.blockNumber)
            .push(log)
      )
    }

    for (const [blockNumber, logs] of rangeLogsTable) {
      await this.logsRepository.push(blockNumber.toString(), ...logs);
    }
  }

  protected async getPointersInLogsRepository(): Promise<{ firstBlockNumber: number, lastBlockNumber: number }> {
    const firstBlockNumber =
      parseInt(
        (await this.logsRepository.getKeysOfListsByPosition(0, 0))
          [0] || '-1'
      )
    const lastBlockNumber =
      parseInt(
        (await this.logsRepository.getKeysOfListsByPosition(-1, -1))
          [0] || '-1'
      )

    return { firstBlockNumber, lastBlockNumber }
  }

  protected async getPastLogsFromNode(payload: GetLogsPayload, options: GetPastLogsFromNodeOptions): Promise<Log[]> {
    const filterByAddressesFlag =
      options.filterByAddressesFlag
        ? !!payload.addresses
        : false

    const blocksRange = {
      toBlockNumber: payload.toBlockNumber,
      fromBlockNumber: payload.fromBlockNumber,
    }

    const incrementStepsState = (state, steps) => {
      state.executionSteps.from = state.executionSteps.to + 1;

      state.blocksRange.to < state.executionSteps.to + steps
        ? state.executionSteps.to = state.blocksRange.to
        : state.executionSteps.to += steps
    }

    const logs = [];

    const state = {
      blocksRange: { from: blocksRange.fromBlockNumber, to: blocksRange.toBlockNumber },
      executionSteps: { from: blocksRange.fromBlockNumber, to: blocksRange.fromBlockNumber + this.options.getLogs.stepRange },
    }

    if (blocksRange.fromBlockNumber + this.options.getLogs.stepRange > blocksRange.toBlockNumber) {
      return await this.web3.eth.getPastLogs({
        address: payload.addresses,
        fromBlock: blocksRange.fromBlockNumber,
        toBlock: blocksRange.toBlockNumber,
      }) as Log[];
    }

    while (state.executionSteps.to < state.blocksRange.to) {
      let newLogs = await this.web3.eth.getPastLogs({
        address: payload.addresses,
        fromBlock: state.executionSteps.from,
        toBlock: state.executionSteps.to,
      }) as Log[];

      logs.push(
        filterByAddressesFlag
          ? this.filterLogsByAddresses(newLogs, payload.addresses)
          : newLogs
      );

      incrementStepsState(state, this.options.getLogs.stepRange);

      if (state.executionSteps.to === state.blocksRange.to) {
        newLogs = await this.web3.eth.getPastLogs({
          address: payload.addresses,
          fromBlock: state.executionSteps.from,
          toBlock: state.executionSteps.to,
        }) as Log[];

        logs.push(...newLogs);
      }
    }

    return logs;
  }

  protected async getPastLogsFromRepository(payload: GetLogsPayload): Promise<Log[]> {
    const logs = await this.logsRepository.getMergedValuesOfListsByScore(payload.fromBlockNumber, payload.toBlockNumber);

    return payload.addresses
      ? this.filterLogsByAddresses(logs, payload.addresses)
      : logs
  }

  protected filterLogsByAddresses(logs: Log[], addresses: string[]): Log[] {
    return logs.filter(
      log =>
        addresses.findIndex(
          address => address.toLowerCase() === log.address.toLowerCase()
        ) !== -1
    )
  }

  public async getBlockNumber(): Promise<number> {
    return await this.web3.eth.getBlockNumber();
  }

  public async getPastLogs(payload: GetPastLogsPayload): Promise<Log[]> {
    const pointersInLogsRepository = await this.getPointersInLogsRepository();

    const toBlockNumber =
      payload.toBlockNumber === 'latest'
        ? await this.getBlockNumber()
        : payload.toBlockNumber as number

    const getLogsPayload = {
      toBlockNumber,
      addresses: payload.addresses,
      fromBlockNumber: payload.fromBlockNumber,
    }

    if (
      pointersInLogsRepository.firstBlockNumber <= getLogsPayload.fromBlockNumber &&
      pointersInLogsRepository.lastBlockNumber >= getLogsPayload.toBlockNumber
    ) {
      return await this.getPastLogsFromRepository(getLogsPayload);
    }

    const logs = await this.getPastLogsFromNode(getLogsPayload, { filterByAddressesFlag: false });

    await this.putLogsInRepository({
      logs: logs,
      ...getLogsPayload,
    });

    return payload.addresses
      ? this.filterLogsByAddresses(logs, payload.addresses)
      : logs
  }
}
