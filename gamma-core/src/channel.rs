/// Where an event is delivered.
///
/// Subscribers listen on exactly one channel and publishers target exactly
/// one. **There is no fan-out:** an event published to `Entity(7)` reaches
/// only subscribers of `Entity(7)`; it does not reach `Global` or `Class`
/// subscribers. A system that wants both publishes twice (or subscribes on
/// both channels). This keeps delivery cost proportional to the audience and
/// the order easy to reason about.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Channel {
    /// Everyone listening to this event globally.
    #[default]
    Global,
    /// One entity (the value is an engine entity handle).
    Entity(u64),
    /// All instances of one class (the value is a class id).
    Class(u64),
}
