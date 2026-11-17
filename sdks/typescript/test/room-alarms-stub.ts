export let pending: bigint | undefined;
export function setAlarm(at: bigint): void {
    pending = at;
}

export function getAlarm(): bigint | undefined { return pending === undefined && quickJS ? null as unknown as undefined : pending; }
export function clearAlarm():void { pending=undefined; }

// QuickJS lowers WIT none to null; StarlingMonkey uses undefined.
export let quickJS = false;
export function useQuickJS(value: boolean) { quickJS = value; }
