#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplayRecordKind {
    Book,
    Trade,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplayMergeEntry {
    pub kind: ReplayRecordKind,
    pub index: usize,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ReplaySortKey {
    ts_event: i64,
    priority: u8,
    ts_init: i64,
    source_order: usize,
}

pub fn replay_merge_plan(
    book_ts_events: &[i64],
    book_ts_inits: &[i64],
    trade_ts_events: &[i64],
    trade_ts_inits: &[i64],
) -> Result<Vec<ReplayMergeEntry>, String> {
    if book_ts_events.len() != book_ts_inits.len() {
        return Err(format!(
            "book timestamp columns have inconsistent lengths: ts_event={}, ts_init={}",
            book_ts_events.len(),
            book_ts_inits.len()
        ));
    }
    if trade_ts_events.len() != trade_ts_inits.len() {
        return Err(format!(
            "trade timestamp columns have inconsistent lengths: ts_event={}, ts_init={}",
            trade_ts_events.len(),
            trade_ts_inits.len()
        ));
    }

    if replay_side_is_sorted(book_ts_events, book_ts_inits)
        && replay_side_is_sorted(trade_ts_events, trade_ts_inits)
    {
        return Ok(replay_linear_merge_plan(
            book_ts_events,
            book_ts_inits,
            trade_ts_events,
            trade_ts_inits,
        ));
    }

    let mut entries: Vec<(ReplaySortKey, ReplayMergeEntry)> =
        Vec::with_capacity(book_ts_events.len() + trade_ts_events.len());
    for (index, (&ts_event, &ts_init)) in book_ts_events.iter().zip(book_ts_inits).enumerate() {
        entries.push((
            ReplaySortKey {
                ts_event,
                priority: 0,
                ts_init,
                source_order: index,
            },
            ReplayMergeEntry {
                kind: ReplayRecordKind::Book,
                index,
            },
        ));
    }
    let source_offset = book_ts_events.len();
    for (index, (&ts_event, &ts_init)) in trade_ts_events.iter().zip(trade_ts_inits).enumerate() {
        entries.push((
            ReplaySortKey {
                ts_event,
                priority: 1,
                ts_init,
                source_order: source_offset + index,
            },
            ReplayMergeEntry {
                kind: ReplayRecordKind::Trade,
                index,
            },
        ));
    }
    entries.sort_by_key(|(key, _entry)| *key);
    Ok(entries.into_iter().map(|(_key, entry)| entry).collect())
}

fn replay_side_is_sorted(ts_events: &[i64], ts_inits: &[i64]) -> bool {
    ts_events
        .iter()
        .zip(ts_inits)
        .map(|(&ts_event, &ts_init)| (ts_event, ts_init))
        .try_fold(None, |previous, current| {
            if previous.is_some_and(|previous| current < previous) {
                None
            } else {
                Some(Some(current))
            }
        })
        .is_some()
}

fn replay_linear_merge_plan(
    book_ts_events: &[i64],
    book_ts_inits: &[i64],
    trade_ts_events: &[i64],
    trade_ts_inits: &[i64],
) -> Vec<ReplayMergeEntry> {
    let mut plan = Vec::with_capacity(book_ts_events.len() + trade_ts_events.len());
    let mut book_index = 0;
    let mut trade_index = 0;
    while book_index < book_ts_events.len() || trade_index < trade_ts_events.len() {
        let take_book = if trade_index >= trade_ts_events.len() {
            true
        } else if book_index >= book_ts_events.len() {
            false
        } else {
            (book_ts_events[book_index], 0_u8, book_ts_inits[book_index])
                <= (
                    trade_ts_events[trade_index],
                    1_u8,
                    trade_ts_inits[trade_index],
                )
        };
        if take_book {
            plan.push(ReplayMergeEntry {
                kind: ReplayRecordKind::Book,
                index: book_index,
            });
            book_index += 1;
        } else {
            plan.push(ReplayMergeEntry {
                kind: ReplayRecordKind::Trade,
                index: trade_index,
            });
            trade_index += 1;
        }
    }
    plan
}

#[cfg(test)]
mod tests {
    use super::{ReplayRecordKind, replay_merge_plan};

    #[test]
    fn replay_merge_plan_matches_book_before_trade_sort_key() {
        let plan = replay_merge_plan(&[10, 5, 10], &[30, 5, 20], &[10, 5], &[1, 6]).unwrap();

        assert_eq!(
            plan.iter()
                .map(|entry| (entry.kind, entry.index))
                .collect::<Vec<_>>(),
            vec![
                (ReplayRecordKind::Book, 1),
                (ReplayRecordKind::Trade, 1),
                (ReplayRecordKind::Book, 2),
                (ReplayRecordKind::Book, 0),
                (ReplayRecordKind::Trade, 0),
            ]
        );
    }

    #[test]
    fn replay_merge_plan_uses_linear_merge_for_sorted_inputs() {
        let plan = replay_merge_plan(&[5, 10, 10], &[5, 20, 30], &[5, 10], &[6, 1]).unwrap();

        assert_eq!(
            plan.iter()
                .map(|entry| (entry.kind, entry.index))
                .collect::<Vec<_>>(),
            vec![
                (ReplayRecordKind::Book, 0),
                (ReplayRecordKind::Trade, 0),
                (ReplayRecordKind::Book, 1),
                (ReplayRecordKind::Book, 2),
                (ReplayRecordKind::Trade, 1),
            ]
        );
    }

    #[test]
    fn replay_merge_plan_rejects_mismatched_lengths() {
        assert!(replay_merge_plan(&[1], &[], &[], &[]).is_err());
        assert!(replay_merge_plan(&[], &[], &[1], &[]).is_err());
    }
}
