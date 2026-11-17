export let pending: bigint | undefined;
export function setAlarm(at: bigint): void {
    pending = at;
}

export function getAlarm(): bigint | undefined { return pending; }
export function clearAlarm():void { pending=undefined; }
