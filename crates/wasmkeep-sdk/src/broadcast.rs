//! Reconnecting room (ADR 0040 R2–R4). Deploy the room internally; fronts
//! use [`Client`] for publishes and gap fetches. No edge-to-room calls.
//! Keep one [`Core`] per component instance, never per request. The first
//! operation after a cold start seals an epoch durably, including a history
//! read: a reconnect must learn losses even before the next publisher arrives.
/// Position within the ordered stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Position {
    pub e: u64,
    pub n: u64,
}
/// A durable epoch boundary; numbers above base in older epochs may be void.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Epoch {
    pub e: u64,
    pub base: u64,
}
#[derive(Clone, Debug)]
pub struct Publish {
    pub body: String,
    pub id: Option<String>,
}
#[derive(Clone, Debug)]
pub struct Message {
    pub position: Position,
    pub at: u64,
    pub id: Option<String>,
    pub body: String,
}
#[derive(Clone, Debug)]
pub struct History {
    pub head: Position,
    pub lost: Option<Epoch>,
    pub reset: Option<Position>,
    pub messages: Vec<Message>,
    pub more: bool,
}
#[cfg(feature = "actor")]
mod core;
#[cfg(feature = "actor")]
pub use core::{Core, Host, Store};
mod protocol;
pub use protocol::Client;
#[cfg(feature = "actor")]
pub use protocol::{Room, handler};
#[cfg(feature = "actor")]
mod edge;
#[cfg(all(test, feature = "actor"))]
#[path = "broadcast/edge_tests.rs"]
mod edge_tests;
#[cfg(feature = "actor")]
pub use edge::{Ack, Edge, HostSockets, Item, Push, SendResult, SocketIo};
#[cfg(feature = "actor")]
mod edge_actor;
#[cfg(feature = "actor")]
pub use edge_actor::EdgeActor;
#[cfg(feature = "actor")]
mod round;
#[cfg(feature = "actor")]
pub use round::Round;
#[cfg(feature = "actor")]
mod scaling;
#[cfg(feature = "actor")]
pub use scaling::{
    ScaleChild, ScaleCommand, ScaleHost, ScaleIo, ScaleKind, ScaleOp, ScaleState, Scaling,
    scale_decision,
};
