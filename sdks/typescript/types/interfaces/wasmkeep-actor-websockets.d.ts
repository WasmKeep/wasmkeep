declare module 'wasmkeep:actor/websockets@0.2.0' {
  /**
   * Queues `msg` on the connection.
   */
  export function send(conn: bigint, msg: Message): void;
  /**
   * Closes the connection with a close code (1000 = normal) and reason.
   */
  export function close(conn: bigint, code: number, reason: string): void;
  /**
   * This actor's open connections, oldest first.
   */
  export function connections(): BigUint64Array;
  /**
   * A WebSocket message.
   */
  export type Message = MessageText | MessageBinary;
  export interface MessageText {
    tag: 'text',
    val: string,
  }
  export interface MessageBinary {
    tag: 'binary',
    val: Uint8Array,
  }
  export type SendError = SendErrorUnknownConnection | SendErrorBackpressure;
  /**
   * The connection is closed, or belongs to another actor.
   */
  export interface SendErrorUnknownConnection {
    tag: 'unknown-connection',
  }
  /**
   * The client is not reading fast enough; the message was dropped.
   */
  export interface SendErrorBackpressure {
    tag: 'backpressure',
  }
}
