# Saga rollback example

This example models a transaction as a saga: each successful business action
records a compensating action in a durable JSON journal. If a later pipeline
step fails, a Caravel run guard applies those compensations in reverse order.

Run the intentionally failing scenario:

```bash
python -m examples.saga_rollback
```

The command raises the simulated error, but
`data/output/saga_rollback_example/external_state.json` is restored to its
initial values. The adjacent `compensation_journal.json` records the actions
and has status `compensated`.

Run the successful scenario:

```bash
python -m examples.saga_rollback --succeed
```

The external state then contains the reduced inventory and account balance plus
the created shipment. The journal has status `committed`, and the intended
pipeline result is written to:

```text
data/output/saga_rollback_example/output/saga_rollback_example/saga-1/
  001-fulfillment/004-finalize_order/order/order.json
```

This is not an ACID transaction. Other processes can observe intermediate
state, a process crash can occur between a mutation and its journal write, and
a real compensation can fail. Production sagas need idempotency keys, atomic
or transactional journal persistence, retryable compensations, and operator
recovery for incomplete journals.
