export interface DEXQuote {
  dex: string;
  tokenIn: string;
  tokenOut: string;
  amountIn: bigint;
  amountOut: bigint;
  priceImpactPct: number;
  gasEstimate: bigint;
  route: string[];
  timestamp: number;
  validUntil: number;
}

export interface SwapRequest {
  dex: string;
  tokenIn: string;
  tokenOut: string;
  amountIn: bigint;
  maxSlippageBps: number;
  recipient: string;
  deadline: number;
}

export interface SwapResult {
  txHash: string;
  dex: string;
  tokenIn: string;
  tokenOut: string;
  amountIn: bigint;
  amountOut: bigint;
  gasUsed: bigint;
  status: "success" | "reverted" | "pending";
  timestamp: number;
}

export interface PoolState {
  address: string;
  token0: string;
  token1: string;
  fee: number;
  sqrtPriceX96: bigint;
  tick: number;
  liquidity: bigint;
  token0Price: number;
  token1Price: number;
}

export interface NetworkConfig {
  rpcHttp: string;
  rpcWs?: string;
  chainId: number;
  name: string;
}

export interface DEXConfig {
  network: NetworkConfig;
  maxSlippageBps: number;
  walletPrivateKey?: string;
  mevProtection?: {
    enableFlashbots?: boolean;
    enablePrivateMempool?: boolean;
    maxSlippage?: number;
    minPriorityFee?: string;
    blockTolerance?: number;
    sandwichDetection?: boolean;
    dynamicSlippage?: boolean;
    maxGasPrice?: string;
  };
}

export type LogLevel = "debug" | "info" | "warn" | "error";
