snippets i asked for

// Create outpoints for all outputs upfront
let all_outpoints: Vec<OutPoint> = tx.output
    .iter()
    .enumerate()
    .map(|(vout, _)| OutPoint::new(txid, vout as u32))
    .collect();

// Filter to spendable ones for lookup
let spendable_outpoints: BTreeSet<_> = tx.output
    .iter()
    .enumerate()
    .filter(|(_, txout)| is_spendable(txout))
    .map(|(vout, _)| all_outpoints[vout].clone()) // or just recreate here
    .collect();

// Lookup spends
let mut chain_spends = self.chain.lookup_spends(spendable_outpoints);
let mempool = self.mempool();

// Final iteration using pre-created outpoints
tx.output
    .iter()
    .enumerate()
    .map(|(vout, txout)| {
        if is_spendable(txout) {
            let outpoint = &all_outpoints[vout]; // Use pre-created outpoint
            chain_spends.remove(outpoint)
                .or_else(|| mempool.lookup_spend(outpoint))
        } else {
            None
        }
    })
    .collect()



-----

**Nit: Consider creating outpoints upfront to avoid duplicate allocation**

Currently we filter spendable outputs and create outpoints (lines 168-170), then recreate the same outpoints again during the final iteration (line 196).

We could create a `Vec<Option<OutPoint>>` upfront where unspendable outputs are `None` and spendable ones are `Some(outpoint)`. Then:
1. Collect the `Some` values into the `BTreeSet` for lookup
2. Use the vector indices in the final iteration to reuse the outpoints

This avoids both duplicate outpoint creation AND creating outpoints for unspendable outputs.

```rust
let outpoints: Vec<Option<OutPoint>> = tx.output
    .iter()
    .enumerate()
    .map(|(vout, txout)| {
        is_spendable(txout).then(|| OutPoint::new(txid, vout as u32))
    })
    .collect();

let spendable_set: BTreeSet<_> = outpoints.iter().flatten().cloned().collect();
let mut chain_spends = self.chain.lookup_spends(spendable_set);
let mempool = self.mempool();

outpoints
    .iter()
    .filter_map(|opt_outpoint| {
        opt_outpoint.as_ref().and_then(|outpoint| {
            chain_spends.remove(outpoint)
                .or_else(|| mempool.lookup_spend(outpoint))
        })
    })
    .collect()

This gets you the best of both worlds - no duplicate work at all!