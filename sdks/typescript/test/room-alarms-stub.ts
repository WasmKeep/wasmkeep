export let pending: bigint | undefined;
export function setAlarm(at: bigint): void {
    pending = at;
}
