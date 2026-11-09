declare module 'wasmkeep:actor/types@0.2.0' {
  /**
   * A SQL value, mapping one to one onto SQLite's storage classes.
   */
  export type Value = ValueNull | ValueInteger | ValueReal | ValueText | ValueBlob;
  export interface ValueNull {
    tag: 'null',
  }
  export interface ValueInteger {
    tag: 'integer',
    val: bigint,
  }
  export interface ValueReal {
    tag: 'real',
    val: number,
  }
  export interface ValueText {
    tag: 'text',
    val: string,
  }
  export interface ValueBlob {
    tag: 'blob',
    val: Uint8Array,
  }
  /**
   * The result set of a query: column names, then one list of values per
   * row, in column order.
   */
  export interface Rows {
    columns: Array<string>,
    values: Array<Array<Value>>,
  }
  /**
   * Why a statement failed.
   * # Variants
   * 
   * ## `"syntax"`
   * 
   * The SQL did not parse, or referred to a missing table or column.
   * ## `"constraint"`
   * 
   * A constraint failed (UNIQUE, NOT NULL, CHECK, FOREIGN KEY).
   * ## `"bind"`
   * 
   * Parameter count or types do not match the statement.
   * ## `"multiple-statements"`
   * 
   * The text held more than one statement; send one at a time.
   * ## `"quota-exceeded"`
   * 
   * The database reached its size cap (default 1 GB); reads still work.
   * ## `"not-allowed"`
   * 
   * The statement tried something actors may not do (ATTACH, a
   * PRAGMA outside the allowed set, loading extensions).
   * ## `"internal"`
   * 
   * The host failed (disk, I/O). The event should fail.
   */
  export type SqlErrorKind = "syntax" | "constraint" | "bind" | "multiple-statements" | "quota-exceeded" | "not-allowed" | "internal";
  export interface SqlError {
    kind: SqlErrorKind,
    /**
     * Human-readable detail, usually SQLite's own message.
     */
    message: string,
  }
}
