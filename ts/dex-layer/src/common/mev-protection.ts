import { ethers } from 'ethers';
import winston from 'winston';

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [new winston.transports.Console()]
});

export interface MEVProtectionConfig {
  enableFlashbots: boolean;
  enablePrivateMempool: boolean;
  maxSlippage: number;
  minPriorityFee: string;
  blockTolerance: number;
  sandwichDetection: boolean;
  dynamicSlippage: boolean;
  maxGasPrice: string;
}

export interface SandwichAttack {
  detected: boolean;
  fronthunningOrder?: ethers.Transaction;
  backrunningOrder?: ethers.Transaction;
  profit: string;
  gasSpent: string;
}

export interface PrivateTxRequest {
  to: string;
  data: string;
  value: string | bigint;
  gasLimit: string | bigint;
  maxFeePerGas?: string | bigint;
  maxPriorityFeePerGas?: string | bigint;
}

export class MEVProtectionService {
  private config: MEVProtectionConfig;
  private flashbotsProvider: any = null;
  private provider: ethers.JsonRpcProvider;

  constructor(
    provider: ethers.JsonRpcProvider,
    config: Partial<MEVProtectionConfig> = {}
  ) {
    this.provider = provider;
    this.config = {
      enableFlashbots: config.enableFlashbots ?? true,
      enablePrivateMempool: config.enablePrivateMempool ?? true,
      maxSlippage: config.maxSlippage ?? 0.005,
      minPriorityFee: config.minPriorityFee ?? '2000000000',
      blockTolerance: config.blockTolerance ?? 3,
      sandwichDetection: config.sandwichDetection ?? true,
      dynamicSlippage: config.dynamicSlippage ?? true,
      maxGasPrice: config.maxGasPrice ?? '100000000000'
    };
  }

  async initialize(flashbotsSigner?: ethers.Wallet): Promise<void> {
    if (this.config.enableFlashbots && flashbotsSigner) {
      try {
        const FlashbotsBundleProvider = await import('@flashbots/ethers-provider-bundle').then(m => m.FlashbotsBundleProvider);

        this.flashbotsProvider = await FlashbotsBundleProvider.create(
          this.provider,
          flashbotsSigner,
          'https://relay.flashbots.net'
        );

        logger.info('Flashbots provider initialized successfully');
      } catch (error) {
        logger.error('Failed to initialize Flashbots provider:', error);
        this.config.enableFlashbots = false;
      }
    }
  }

  async detectSandwichAttack(tx: ethers.Transaction): Promise<SandwichAttack> {
    if (!this.config.sandwichDetection) {
      return { detected: false, profit: '0', gasSpent: '0' };
    }

    try {
      const pendingTxs = await this.provider.send('txpool_content', []);
      const pendingEntries = this.extractPendingTransactions(pendingTxs);
      const targetGas = this.extractGasPrice(tx);
      const targetPrefix = this.extractDataPrefix((tx as any).data);

      const potentialAttacks: SandwichAttack[] = [];

      for (const pendingTx of pendingEntries) {
        if (!pendingTx || (pendingTx as any).hash === (tx as any).hash) continue;
        if (!this.sameAddress((pendingTx as any).to, tx.to)) continue;

        const pendingGas = this.extractGasPrice(pendingTx);
        const pendingPrefix = this.extractDataPrefix((pendingTx as any).data);
        const similarCall = !!targetPrefix && pendingPrefix === targetPrefix;
        const similarValue = this.isComparableValue((pendingTx as any).value, (tx as any).value);
        const isFrontrun = pendingGas > targetGas && similarCall;
        const isBackrun = pendingGas < targetGas && (similarCall || similarValue);

        if (isFrontrun || isBackrun) {
          potentialAttacks.push({
            detected: true,
            fronthunningOrder: isFrontrun ? pendingTx as ethers.Transaction : undefined,
            backrunningOrder: isBackrun ? pendingTx as ethers.Transaction : undefined,
            profit: '0',
            gasSpent: String((pendingTx as any).gas ?? (pendingTx as any).gasLimit ?? '0')
          });
        }
      }

      if (potentialAttacks.length > 0) {
        logger.warn(`Detected ${potentialAttacks.length} potential sandwich attacks`);
        return potentialAttacks[0];
      }

      return { detected: false, profit: '0', gasSpent: '0' };
    } catch (error) {
      logger.error('Error detecting sandwich attack:', error);
      return { detected: false, profit: '0', gasSpent: '0' };
    }
  }

  async sendPrivateTransaction(request: PrivateTxRequest): Promise<ethers.TransactionResponse> {
    if (!this.config.enablePrivateMempool) {
      return this.sendPublicTransaction(request);
    }

    try {
      if (this.config.enableFlashbots && this.flashbotsProvider) {
        return await this.sendFlashbotsBundle(request);
      }

      const txHash = await this.provider.send('eth_sendPrivateTransaction', [{
        to: request.to,
        data: request.data,
        value: request.value || '0x0',
        gas: request.gasLimit,
        maxFeePerGas: request.maxFeePerGas,
        maxPriorityFeePerGas: request.maxPriorityFeePerGas
      }]);

      logger.info(`Private transaction sent: ${txHash}`);

      const receipt = await this.provider.waitForTransaction(txHash, this.config.blockTolerance);
      return receipt as any;
    } catch (error) {
      logger.error('Private transaction failed, falling back to public:', error);
      return this.sendPublicTransaction(request);
    }
  }

  private async sendFlashbotsBundle(request: PrivateTxRequest): Promise<ethers.TransactionResponse> {
    if (!this.flashbotsProvider) {
      throw new Error('Flashbots provider not initialized');
    }

    const signedBundle = await this.flashbotsProvider.signBundle([
      {
        signer: await this.flashbotsProvider.getSigner(),
        transaction: request
      }
    ]);

    const simulation = await this.flashbotsProvider.simulate(signedBundle);

    if (!simulation.firstRelevancy || !simulation.firstRelevancy.isError()) {
      const bundleStats = await this.flashbotsProvider.sendRawBundle(signedBundle, await this.provider.getBlockNumber() + 1);

      logger.info('Flashbots bundle sent');

      const txResponse = await bundleStats.wait();
      return txResponse as any;
    } else {
      throw new Error(`Bundle simulation failed: ${simulation.firstRelevancy.error}`);
    }
  }

  private async sendPublicTransaction(request: PrivateTxRequest): Promise<ethers.TransactionResponse> {
    const signer = await this.provider.getSigner();
    const tx = await signer.sendTransaction({
      to: request.to as `0x${string}`,
      data: request.data as `0x${string}`,
      value: request.value ? BigInt(request.value) : 0n,
      gasLimit: request.gasLimit ? BigInt(request.gasLimit) : undefined,
      maxFeePerGas: request.maxFeePerGas ? BigInt(request.maxFeePerGas) : undefined,
      maxPriorityFeePerGas: request.maxPriorityFeePerGas ? BigInt(request.maxPriorityFeePerGas) : undefined
    });

    logger.info(`Public transaction sent: ${tx.hash}`);
    return tx;
  }

  private extractPendingTransactions(pendingTxs: any): any[] {
    const pendingRoot = pendingTxs && typeof pendingTxs === 'object' ? (pendingTxs.pending ?? {}) : {};
    const flat: any[] = [];

    for (const accountTxs of Object.values(pendingRoot as Record<string, any>)) {
      if (!accountTxs || typeof accountTxs !== 'object') continue;
      for (const pendingTx of Object.values(accountTxs as Record<string, any>)) {
        if (pendingTx) flat.push(pendingTx);
      }
    }

    return flat;
  }

  private extractGasPrice(tx: any): bigint {
    const raw = tx?.gasPrice ?? tx?.maxFeePerGas ?? tx?.maxPriorityFeePerGas ?? 0;
    try {
      return BigInt(raw);
    } catch {
      return 0n;
    }
  }

  private extractDataPrefix(data: any): string {
    const raw = typeof data === 'string' ? data.toLowerCase() : '';
    return raw.length >= 10 ? raw.slice(0, 10) : raw;
  }

  private sameAddress(a?: string | null, b?: string | null): boolean {
    if (!a || !b) return false;
    try {
      return ethers.getAddress(a) === ethers.getAddress(b);
    } catch {
      return String(a).toLowerCase() === String(b).toLowerCase();
    }
  }

  private isComparableValue(a: any, b: any): boolean {
    try {
      return BigInt(a ?? 0) === BigInt(b ?? 0);
    } catch {
      return String(a ?? '') === String(b ?? '');
    }
  }

  calculateDynamicSlippage(baseSlippage: number, volatility: number, orderSize: number, liquidity: number): number {
    if (!this.config.dynamicSlippage) {
      return baseSlippage;
    }

    const sizeFactor = Math.min(orderSize / liquidity, 0.5);
    const volatilityFactor = Math.min(volatility / 100, 0.5);

    const adjustedSlippage = baseSlippage * (1 + sizeFactor + volatilityFactor);

    return Math.min(adjustedSlippage, this.config.maxSlippage * 2);
  }

  async estimatePriorityFee(): Promise<string> {
    try {
      const feeData = await this.provider.getFeeData();

      const basePriorityFee = feeData.maxPriorityFeePerGas || 0n;
      const minFee = BigInt(this.config.minPriorityFee);

      return (basePriorityFee > minFee ? basePriorityFee : minFee).toString();
    } catch (error) {
      logger.error('Error estimating priority fee:', error);
      return this.config.minPriorityFee;
    }
  }

  async checkGasConditions(): Promise<boolean> {
    try {
      const feeData = await this.provider.getFeeData();
      const maxGasPrice = BigInt(this.config.maxGasPrice);

      if (feeData.gasPrice && feeData.gasPrice > maxGasPrice) {
        logger.warn(`Gas price too high: ${feeData.gasPrice.toString()}`);
        return false;
      }

      if (feeData.maxFeePerGas && feeData.maxFeePerGas > maxGasPrice) {
        logger.warn(`Max fee per gas too high: ${feeData.maxFeePerGas.toString()}`);
        return false;
      }

      return true;
    } catch (error) {
      logger.error('Error checking gas conditions:', error);
      return false;
    }
  }

  async executeWithMEVProtection(
    txRequest: PrivateTxRequest,
    slippage: number,
    volatility?: number,
    orderSize?: number,
    liquidity?: number
  ): Promise<ethers.TransactionReceipt> {
    const attack = await this.detectSandwichAttack(txRequest as any);

    if (attack.detected) {
      logger.warn('Sandwich attack detected, adjusting strategy');

      const adjustedSlippage = this.calculateDynamicSlippage(
        slippage * 1.5,
        volatility || 0.01,
        orderSize || 1,
        liquidity || 1000
      );

      const newTx = this.adjustForSlippage(txRequest, adjustedSlippage);
      const tx = await this.sendPrivateTransaction(newTx);
      const receipt = await tx.wait();
      if (!receipt) {
        throw new Error('Protected transaction did not produce a receipt');
      }

      return receipt;
    }

    const adjustedSlippage = this.calculateDynamicSlippage(
      slippage,
      volatility || 0.01,
      orderSize || 1,
      liquidity || 1000
    );

    const newTx = this.adjustForSlippage(txRequest, adjustedSlippage);
    const tx = await this.sendPrivateTransaction(newTx);
    const receipt = await tx.wait();
    if (!receipt) {
      throw new Error('Protected transaction did not produce a receipt');
    }

    return receipt;
  }

  private adjustForSlippage(request: PrivateTxRequest, slippage: number): PrivateTxRequest {
    const encodedTx = ethers.AbiCoder.defaultAbiCoder().decode(
      ['uint256', 'address', 'address', 'uint256', 'uint256', 'uint256'],
      ethers.hexlify(ethers.getBytes(request.data)).substring(10)
    );

    const [amountOut, tokenIn, , amountIn, ,] = encodedTx;

    const adjustedAmountOut = (BigInt(amountOut) * BigInt(Math.floor((1 - slippage) * 10000))) / 10000n;

    const newData = ethers.AbiCoder.defaultAbiCoder().encode(
      ['uint256', 'address', 'address', 'uint256', 'uint256', 'uint256'],
      [adjustedAmountOut, tokenIn, encodedTx[2], amountIn, encodedTx[4], encodedTx[5]]
    );

    return {
      ...request,
      data: request.data.substring(0, 10) + ethers.hexlify(newData).substring(2)
    };
  }

  getConfig(): MEVProtectionConfig {
    return { ...this.config };
  }

  updateConfig(updates: Partial<MEVProtectionConfig>): void {
    this.config = { ...this.config, ...updates };
    logger.info('MEV protection config updated');
  }
}

export class EigenPhiAnalyzer {
  private apiKey: string;
  private baseUrl: string = 'https://api.eigenphi.io/api/v1';

  constructor(apiKey: string) {
    this.apiKey = apiKey;
  }

  async analyzeMEVOpportunity(txHash: string): Promise<any> {
    try {
      const response = await fetch(`${this.baseUrl}/mev/opportunity/${txHash}`, {
        headers: {
          'Authorization': `Bearer ${this.apiKey}`,
          'Content-Type': 'application/json'
        }
      });

      if (!response.ok) {
        throw new Error(`EigenPhi API error: ${response.statusText}`);
      }

      return await response.json();
    } catch (error) {
      logger.error('Error analyzing MEV opportunity:', error);
      return null;
    }
  }

  async getTopMEVBots(limit: number = 10): Promise<any[]> {
    try {
      const response = await fetch(`${this.baseUrl}/mev/bots?limit=${limit}`, {
        headers: {
          'Authorization': `Bearer ${this.apiKey}`,
          'Content-Type': 'application/json'
        }
      });

      if (!response.ok) {
        throw new Error(`EigenPhi API error: ${response.statusText}`);
      }

      return await response.json() as any[];
    } catch (error) {
      logger.error('Error fetching top MEV bots:', error);
      return [];
    }
  }
}
