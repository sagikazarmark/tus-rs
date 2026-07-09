# No backend-bundling trait; keep four explicit generics

Status: Accepted

## Context

The public `tus-axum` surface carries a four-generic `<S, St, L, H>` clause
(`Storage`, `StateStore`, `Locker`, `HookExecutor`) on `create_router`,
`TusRouter`, `TusState`, `TusProtocol`, and any custom handler. A `TusBackend`
bundling trait with four associated types could collapse this to a single
`B: TusBackend` bound. This had to be decided before 1.0 because it is a breaking
arity change to every generic type — not an additive change — so it cannot be
introduced later without a major bump or duplicate parallel APIs.

## Decision

Keep the four explicit generics. Do not introduce a `TusBackend` bundling trait.

The verbosity only reaches advanced users: the common path
(`create_router(TusState::new(protocol), options)`) infers all four generics and
writes zero bounds. The four-bound `where` block is hand-written only by
custom-handler authors. Four ordinary generics also give better inference and
error messages than associated-type projections, and a blanket
`impl<S, St, L, H> TusBackend for Bundle<S, St, L, H>` would reintroduce the four
names at the impl site anyway — the types move, they do not disappear.

## Consequences

Custom-handler authors write the full four-bound `where` clause (see the
`state.rs` example). This is accepted as the cost of conventional, well-diagnosed
generics. A bundling trait is a deliberate non-goal for 1.0, not an oversight.
