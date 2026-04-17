import {
  JsonRpcProvider,
  Contract,
  parseUnits,
  formatUnits,
  Wallet,
  BigNumberish,
} from "ethers";
import type { DEXConfig, DEXQuote, SwapResult } from "../common/types";
import { logger } from "../common/logger";
import { MEVProtectionService } from "../common/mev-protection";

const QUOTER_V2_ABI = [
  "function quoteExactInputSingle(tuple(address tokenIn, address tokenOut, uint256 amountIn, uint24 fee, uint160 sqrtPriceLimitX96) params) external returns (uint256 amountOut, uint160 sqrtPriceX96After, uint32 initializedTicksCrossed, uint256 gasEstimate)",
  "function quoteExactInput(bytes path, uint256 amountIn) external returns (uint256 amountOut)",
];

const SWAP_ROUTER_ABI = [
  "function exactInputSingle(tuple(address tokenIn, address tokenOut, uint24 fee, address recipient, uint256 amountIn, uint256 amountOutMinimum, uint160 sqrtPriceLimitX96) params) external payable returns (uint256 amountOut)",
  "function exactInput(tuple(bytes path, address recipient, uint256 deadline, uint256 amountIn, uint256 amountOutMinimum) params) external payable returns (uint256 amountOut)",
  "function multicall(uint256 deadline, bytes[] data) external payable returns (bytes[])",
];

const FACTORY_ABI = [
  "function getPool(address tokenA, address tokenB, uint24 fee) external view returns (address pool)",
];

const POOL_ABI = [
  "function slot0() external view returns (uint160 sqrtPriceX96, int24 tick, uint16 observationIndex, uint16 observationCardinality, uint16 observationCardinalityNext, uint8 feeProtocol, bool unlocked)",
  "function liquidity() external view returns (uint128)",
  "function token0() external view returns (address)",
  "function token1() external view returns (address)",
  "function ticks(int24 tick) external view returns (uint128 liquidityGross, int128 liquidityNet, uint256 feeGrowthOutside0X128, uint256 feeGrowthOutside1X128, int56 tickCumulativeOutside, uint160 secondsPerLiquidityOutsideX128, uint32 secondsOutside, bool initialized)",
];

const POSITION_MANAGER_ABI = [
  "function positions(uint256 tokenId) external view returns (uint96 nonce, address operator, address token0, address token1, uint24 fee, int24 tickLower, int24 tickUpper, uint128 liquidity, uint256 feeGrowthInside0LastX128, uint256 feeGrowthInside1LastX128, uint128 tokensOwed0, uint128 tokensOwed1)",
];

const UNISWAP_V3_FACTORY = "0x1F98431c8aD98523631AE4a59f267346ea31F984";
const UNISWAP_QUOTER_V2 = "0x61fFE014bA17989E743c5F6cB21bF9697530B21e";
const UNISWAP_SWAP_ROUTER = "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45";
const POSITION_MANAGER = "0xC36442b4a4522E871399CD717aBDD847Ab11FE88";

const FEE_TIERS = [100, 500, 3000, 10000] as const;

interface PoolInfo {
  address: string;
  fee: number;
  liquidity: bigint;
  sqrtPriceX96: bigint;
  tick: number;
}

interface HopRoute {
  pools: PoolInfo[];
  amountIn: bigint;
  amountOut: bigint;
  priceImpact: number;
}

interface ConcentratedLiquidityInfo {
  tokenId: bigint;
  tickLower: number;
  tickUpper: number;
  liquidity: bigint;
  tokensOwed0: bigint;
  tokensOwed1: bigint;
}

export class UniswapV3Executor {
  private provider: JsonRpcProvider;
  private quoter: Contract;
  private factory: Contract;
  private positionManager?: Contract;
  private config: DEXConfig;
  private wallet?: Wallet;
  private mevProtection?: MEVProtectionService;
  private poolCache: Map<string, PoolInfo> = new Map();
  private liquidityCache: Map<string, bigint> = new Map();

  constructor(config: DEXConfig) {
    this.config = config;
    this.provider = new JsonRpcProvider(config.network.rpcHttp);
    this.quoter = new Contract(UNISWAP_QUOTER_V2, QUOTER_V2_ABI, this.provider);
    this.factory = new Contract(UNISWAP_V3_FACTORY, FACTORY_ABI, this.provider);

    if (config.walletPrivateKey) {
      this.wallet = new Wallet(config.walletPrivateKey, this.provider);
      this.mevProtection = new MEVProtectionService(this.provider, config.mevProtection);
    }

    this.positionManager = new Contract(POSITION_MANAGER, POSITION_MANAGER_ABI, this.provider);
  }

  async initializeMEVProtection(flashbotsSigner?: Wallet): Promise<void> {
    if (this.mevProtection) {
      await this.mevProtection.initialize(flashbotsSigner);
    }
  }

  async getPoolInfo(tokenA: string, tokenB: string, fee: number): Promise<PoolInfo | null> {
    const cacheKey = `${tokenA}:${tokenB}:${fee}`;

    if (this.poolCache.has(cacheKey)) {
      return this.poolCache.get(cacheKey)!;
    }

    try {
      const poolAddress = await this.factory.getPool(tokenA, tokenB, fee);
      if (poolAddress === "0x0000000000000000000000000000000000000000") {
        return null;
      }

      const pool = new Contract(poolAddress, POOL_ABI, this.provider);
      const [slot0, liquidity] = await Promise.all([
        pool.slot0(),
        pool.liquidity(),
      ]);

      const poolInfo: PoolInfo = {
        address: poolAddress,
        fee,
        liquidity,
        sqrtPriceX96: slot0.sqrtPriceX96,
        tick: Number(slot0.tick),
      };

      this.poolCache.set(cacheKey, poolInfo);
      return poolInfo;
    } catch (err) {
      logger.error(`Error getting pool info for ${tokenA}/${tokenB} fee=${fee}:`, err);
      return null;
    }
  }

  async getConcentratedLiquidity(tokenA: string, tokenB: string): Promise<ConcentratedLiquidityInfo[]> {
    const results: ConcentratedLiquidityInfo[] = [];

    if (!this.wallet) {
      return results;
    }

    try {
      const ownerAddress = await this.wallet.getAddress();

      for (const fee of FEE_TIERS) {
        const poolInfo = await this.getPoolInfo(tokenA, tokenB, fee);
        if (!poolInfo) continue;

        const pool = new Contract(poolInfo.address, POOL_ABI, this.provider);
        const [token0, token1] = await Promise.all([
          pool.token0(),
          pool.token1(),
        ]);

        if ((token0.toLowerCase() !== tokenA.toLowerCase() && token0.toLowerCase() !== tokenB.toLowerCase()) ||
            (token1.toLowerCase() !== tokenA.toLowerCase() && token1.toLowerCase() !== tokenB.toLowerCase())) {
          continue;
        }

        const positions = await this.getPositionsInRange(
          ownerAddress,
          poolInfo,
          -887272,
          887272
        );

        results.push(...positions);
      }
    } catch (err) {
      logger.error(`Error getting concentrated liquidity:`, err);
    }

    return results;
  }

  private async getPositionsInRange(
    owner: string,
    poolInfo: PoolInfo,
    tickLower: number,
    tickUpper: number
  ): Promise<ConcentratedLiquidityInfo[]> {
    const results: ConcentratedLiquidityInfo[] = [];

    try {
      const pool = new Contract(poolInfo.address, POOL_ABI, this.provider);

      const tickSpacing = this.getTickSpacing(poolInfo.fee);
      const startTick = Math.floor(tickLower / tickSpacing) * tickSpacing;
      const endTick = Math.ceil(tickUpper / tickSpacing) * tickSpacing;

      for (let tick = startTick; tick <= endTick; tick += tickSpacing) {
        try {
          const tickData = await pool.ticks(tick);

          if (tickData.initialized) {
            const liquidity = tickData.liquidityGross;

            if (liquidity > 0n) {
              results.push({
                tokenId: 0n,
                tickLower: tick,
                tickUpper: tick + tickSpacing,
                liquidity,
                tokensOwed0: 0n,
                tokensOwed1: 0n,
              });
            }
          }
        } catch (err) {
          continue;
        }
      }
    } catch (err) {
      logger.error(`Error getting positions in range:`, err);
    }

    return results;
  }

  private getTickSpacing(fee: number): number {
    switch (fee) {
      case 100:
        return 1;
      case 500:
        return 10;
      case 3000:
        return 60;
      case 10000:
        return 200;
      default:
        return 60;
    }
  }

  async findMultiHopRoute(
    tokenIn: string,
    tokenOut: string,
    amountIn: bigint,
    maxHops: number = 3,
    intermediateTokens?: string[]
  ): Promise<DEXQuote | null> {
    const commonIntermediateTokens = intermediateTokens || [
      "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
      "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
      "0xdAC17F958D2ee523a2206206994597C13D831ec7",
      "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
    ];

    for (const intermediate of commonIntermediateTokens) {
      if (intermediate.toLowerCase() === tokenIn.toLowerCase() ||
          intermediate.toLowerCase() === tokenOut.toLowerCase()) {
        continue;
      }

      const route = await this.executeMultiHopSwap(
        [tokenIn, intermediate, tokenOut],
        amountIn,
        false
      );

      if (route && route.amountOut > 0n) {
        return route;
      }
    }

    return null;
  }

  async executeMultiHopSwap(
    path: string[],
    amountIn: bigint,
    execute: boolean = true
  ): Promise<DEXQuote | null> {
    if (path.length < 2) {
      return null;
    }

    try {
      const fees = this.calculateFeesForPath(path);

      const pathEncoded = this.encodePath(path, fees);

      const amountOut = await this.quoter.quoteExactInput.staticCall(pathEncoded, amountIn);

      if (!execute) {
        return {
          dex: "uniswap_v3",
          tokenIn: path[0],
          tokenOut: path[path.length - 1],
          amountIn,
          amountOut: amountOut as bigint,
          priceImpactPct: this._estimatePriceImpact(amountIn, amountOut as bigint),
          gasEstimate: 300000n,
          route: path,
          timestamp: Math.floor(Date.now() / 1000),
          validUntil: Math.floor(Date.now() / 1000) + 30,
        };
      }

      const quote: DEXQuote = {
        dex: "uniswap_v3",
        tokenIn: path[0],
        tokenOut: path[path.length - 1],
        amountIn,
        amountOut: amountOut as bigint,
        priceImpactPct: this._estimatePriceImpact(amountIn, amountOut as bigint),
        gasEstimate: 300000n,
        route: path,
        timestamp: Math.floor(Date.now() / 1000),
        validUntil: Math.floor(Date.now() / 1000) + 30,
      };

      const result = await this.executeSwap(quote);
      return result ? quote : null;
    } catch (err) {
      logger.error(`Multi-hop swap failed for path ${path.join(' -> ')}:`, err);
      return null;
    }
  }

  private encodePath(path: string[], fees: number[]): string {
    const FEE_SIZE = 3;

    const pathBytes: string[] = [];

    for (let i = 0; i < path.length; i++) {
      const addressBytes = path[i].slice(2).toLowerCase();
      pathBytes.push(addressBytes);

      if (i < fees.length) {
        const feeBytes = fees[i].toString(16).padStart(6, '0');
        pathBytes.push(feeBytes);
      }
    }

    return "0x" + pathBytes.join('');
  }

  private calculateFeesForPath(path: string[]): number[] {
    const fees: number[] = [];

    for (let i = 0; i < path.length - 1; i++) {
      const bestPool = this.findBestPoolForPair(path[i], path[i + 1]);
      fees.push(bestPool?.fee || 3000);
    }

    return fees;
  }

  private findBestPoolForPair(tokenA: string, tokenB: string): PoolInfo | null {
    let bestPool: PoolInfo | null = null;
    let bestLiquidity = 0n;

    for (const fee of FEE_TIERS) {
      const cacheKey = `${tokenA}:${tokenB}:${fee}`;
      const poolInfo = this.poolCache.get(cacheKey);

      if (poolInfo && poolInfo.liquidity > bestLiquidity) {
        bestPool = poolInfo;
        bestLiquidity = poolInfo.liquidity;
      }
    }

    return bestPool;
  }

  async getQuote(
    tokenIn: string,
    tokenOut: string,
    amountIn: bigint,
    feeTier?: number
  ): Promise<DEXQuote | null> {
    const feesToTry = feeTier ? [feeTier] : [...FEE_TIERS];
    let bestQuote: DEXQuote | null = null;

    for (const fee of feesToTry) {
      try {
        const poolInfo = await this.getPoolInfo(tokenIn, tokenOut, fee);
        if (!poolInfo) continue;

        const [amountOut, , , gasEstimate] = await this.quoter.quoteExactInputSingle.staticCall({
          tokenIn,
          tokenOut,
          amountIn,
          fee,
          sqrtPriceLimitX96: 0n,
        });

        const priceImpact = this._estimatePriceImpact(amountIn, amountOut as bigint);
        const now = Math.floor(Date.now() / 1000);

        const quote: DEXQuote = {
          dex: "uniswap_v3",
          tokenIn,
          tokenOut,
          amountIn,
          amountOut: amountOut as bigint,
          priceImpactPct: priceImpact,
          gasEstimate: gasEstimate as bigint,
          route: [tokenIn, tokenOut],
          timestamp: now,
          validUntil: now + 30,
        };

        if (bestQuote === null || quote.amountOut > bestQuote.amountOut) {
          bestQuote = quote;
        }
      } catch (err) {
        logger.debug(`Uniswap V3 quote failed for fee=${fee}: ${err}`);
      }
    }

    return bestQuote;
  }

  async executeSwap(quote: DEXQuote): Promise<SwapResult | null> {
    if (!this.wallet) {
      logger.error("Wallet not configured — cannot execute Uniswap swap");
      return null;
    }

    if (this.mevProtection) {
      return await this.executeSwapWithMEV(quote);
    }

    return await this.executeSwapDirect(quote);
  }

  private async executeSwapWithMEV(quote: DEXQuote): Promise<SwapResult | null> {
    if (!this.wallet || !this.mevProtection) {
      return null;
    }

    const router = new Contract(UNISWAP_SWAP_ROUTER, SWAP_ROUTER_ABI, this.wallet);
    const slippageBps = this.config.maxSlippageBps;
    const minOut = (quote.amountOut * BigInt(10000 - slippageBps)) / 10000n;
    const deadline = Math.floor(Date.now() / 1000) + 300;

    try {
      const data = router.interface.encodeFunctionData('exactInputSingle', [{
        tokenIn: quote.tokenIn,
        tokenOut: quote.tokenOut,
        fee: 3000,
        recipient: await this.wallet.getAddress(),
        amountIn: quote.amountIn,
        amountOutMinimum: minOut,
        sqrtPriceLimitX96: 0n,
      }]);

      const priorityFee = await this.mevProtection.estimatePriorityFee();
      const feeData = await this.provider.getFeeData();

      const receipt = await this.mevProtection.executeWithMEVProtection({
        to: UNISWAP_SWAP_ROUTER,
        data,
        value: '0x0',
        gasLimit: quote.gasEstimate * 120n / 100n,
        maxFeePerGas: feeData.maxFeePerGas?.toString(),
        maxPriorityFeePerGas: priorityFee,
      }, slippageBps / 10000);

      return {
        txHash: '0x' + '00'.repeat(32),
        dex: "uniswap_v3",
        tokenIn: quote.tokenIn,
        tokenOut: quote.tokenOut,
        amountIn: quote.amountIn,
        amountOut: quote.amountOut,
        gasUsed: receipt.gasUsed || quote.gasEstimate,
        status: "success",
        timestamp: Math.floor(Date.now() / 1000),
      };
    } catch (err) {
      logger.error(`Uniswap swap with MEV protection failed: ${err}`);
      return await this.executeSwapDirect(quote);
    }
  }

  private async executeSwapDirect(quote: DEXQuote): Promise<SwapResult | null> {
    if (!this.wallet) {
      return null;
    }

    const router = new Contract(UNISWAP_SWAP_ROUTER, SWAP_ROUTER_ABI, this.wallet);
    const slippageBps = this.config.maxSlippageBps;
    const minOut = (quote.amountOut * BigInt(10000 - slippageBps)) / 10000n;
    const deadline = Math.floor(Date.now() / 1000) + 300;

    try {
      const tx = await router.exactInputSingle({
        tokenIn: quote.tokenIn,
        tokenOut: quote.tokenOut,
        fee: 3000,
        recipient: await this.wallet.getAddress(),
        amountIn: quote.amountIn,
        amountOutMinimum: minOut,
        sqrtPriceLimitX96: 0n,
      });
      const receipt = await tx.wait();
      return {
        txHash: receipt.hash,
        dex: "uniswap_v3",
        tokenIn: quote.tokenIn,
        tokenOut: quote.tokenOut,
        amountIn: quote.amountIn,
        amountOut: quote.amountOut,
        gasUsed: BigInt(receipt.gasUsed),
        status: receipt.status === 1 ? "success" : "reverted",
        timestamp: Math.floor(Date.now() / 1000),
      };
    } catch (err) {
      logger.error(`Uniswap swap failed: ${err}`);
      return null;
    }
  }

  private _estimatePriceImpact(amountIn: bigint, amountOut: bigint): number {
    if (amountOut === 0n) return 100;
    const ratio = Number(amountIn) / Number(amountOut);
    return Math.max(0, (ratio - 1) * 100);
  }
}
