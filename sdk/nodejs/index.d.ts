import type { IncomingMessage, ServerResponse } from "node:http";
import type WebSocket from "ws";
export interface Config {
  registry: string;
  app: string;
  weight: number;
  calls: Record<string, string | CallOptions>;
  callRetryTimes: number;
}
export interface CallOptions {
  timeout?: number;
  httpVersion?: 1 | 2;
  ssl?: boolean;
  token?: string;
}
export interface NodeInfo {
  addr: string;
  weight: number;
  usedTimes: number;
  failedTimes: number;
}
export interface RequestContext {
  headers?: Record<string, string | string[] | undefined>;
  clientIP?: string;
  host?: string;
  scheme?: string;
  userAgent?: string;
  signal?: AbortSignal;
}
export interface Result {
  statusCode: number;
  headers: Record<string, string | string[]>;
  body: Buffer;
  node: string;
  native: unknown;
}
export const config: Config;
export const healthPath: string;
export function init(): void;
export function configure(value: Partial<Config>): void;
export function start(addr?: string): Promise<boolean>;
export function stop(): Promise<void>;
export function wait(): Promise<void>;
export function isClient(): boolean;
export function isServer(): boolean;
export function addExternalApp(
  app: string,
  callConf?: string | CallOptions,
): Promise<boolean>;
export function setNode(app: string, addr: string, weight: number): void;
export function getAppNodes(app: string): Record<string, NodeInfo>;
export function setLogger(logger: { error: (error: unknown) => void }): void;
export function setRoute(
  route:
    | ((
        call: {
          method: string;
          app: string;
          path: string;
          data?: unknown;
          headers: Record<string, string | string[]>;
        },
        request: RequestContext | IncomingMessage | undefined,
      ) => void | Promise<void>)
    | undefined,
): void;
export function setLoadBalancer(
  lb:
    | {
        next: (call: unknown, nodes: NodeInfo[], request: unknown) => NodeInfo;
        response: (
          call: unknown,
          node: NodeInfo,
          error: unknown,
          result: Result | undefined,
        ) => void;
      }
    | undefined,
): void;
export function setHealthHandler(
  handler:
    ((request: IncomingMessage, response: ServerResponse) => void) | undefined,
): void;
export function healthHandler(
  request: IncomingMessage,
  response: ServerResponse,
): void;
export function middleware(
  next: (request: IncomingMessage, response: ServerResponse) => void,
): (request: IncomingMessage, response: ServerResponse) => void;
export function newCaller(
  request?: RequestContext | IncomingMessage,
  logger?: unknown,
): Caller;
export class Caller {
  constructor(request?: RequestContext | IncomingMessage, logger?: unknown);
  do(
    method: string,
    app: string,
    path: string,
    data?: unknown,
    headers?: Record<string, string | string[]>,
    withNode?: string,
  ): Promise<Result>;
  get(
    app: string,
    path: string,
    headers?: Record<string, string | string[]>,
  ): Promise<Result>;
  post(
    app: string,
    path: string,
    data?: unknown,
    headers?: Record<string, string | string[]>,
  ): Promise<Result>;
  put(
    app: string,
    path: string,
    data?: unknown,
    headers?: Record<string, string | string[]>,
  ): Promise<Result>;
  delete(
    app: string,
    path: string,
    data?: unknown,
    headers?: Record<string, string | string[]>,
  ): Promise<Result>;
  head(
    app: string,
    path: string,
    headers?: Record<string, string | string[]>,
  ): Promise<Result>;
  doWithNode(
    method: string,
    app: string,
    withNode: string,
    path: string,
    data?: unknown,
    headers?: Record<string, string | string[]>,
  ): Promise<Result>;
  open(
    app: string,
    path: string,
    headers?: Record<string, string | string[]>,
  ): Promise<WebSocket>;
}

export function isRegistered(): boolean;
