import { broadcast } from '@wasmkeep/sdk';
// The sequencer: numbers, the window, history and the pusher's pull. Rounds,
// edges, relays and scaling are the pusher's (src/pusher.ts).
export default broadcast.define();
