import type { Push } from './room-edge';
export interface RelayPush {
    shared: Push;
    children: {
        edge: string;
        push: Push;
        count: number;
    }[];
}
/** Carry the oldest bounded slice once. Later children catch up in subsequent rounds. */
export function packRelay(entries: {
    edge: string;
    push: Push;
}[]): RelayPush {
    if (!entries.length || entries.length > 16)
        throw Error('relay fan-out: 1..16');
    const shared = entries.filter(e => e.push.messages.length).sort((a, b) => a.push.first - b.push.first)[0]?.push ?? entries[0].push;
    return { shared, children: entries.map(({ edge, push }) => ({ edge, push: { ...push, messages: [] }, count: push.recover ? 0 : Math.max(0, Math.min(push.messages.length, shared.first + shared.messages.length - push.first)) })) };
}
export function unpackRelay(value: RelayPush): {
    edge: string;
    push: Push;
}[] {
    if (!value || !Array.isArray(value.children) || !value.children.length || value.children.length > 16 || !Array.isArray(value.shared?.messages) || value.shared.messages.length > 512)
        throw Error('invalid relay');
    const seen = new Set<string>();
    return value.children.map(c => {
        if (typeof c.edge !== 'string' || seen.has(c.edge) || !c.push || !Number.isSafeInteger(c.count) || c.count < 0 || c.count > 512 || c.push.messages?.length !== 0)
            throw Error('invalid relay child');
        seen.add(c.edge);
        const offset = c.push.first - value.shared.first;
        if (c.count && (offset < 0 || offset + c.count > value.shared.messages.length))
            throw Error('invalid relay slice');
        return { edge: c.edge, push: { ...c.push, messages: c.count ? value.shared.messages.slice(offset, offset + c.count) : [] } };
    });
}
