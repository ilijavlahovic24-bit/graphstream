use chrono::NaiveDateTime;
struct Interval{
    start_time:NaiveDateTime,
    end_time:NaiveDateTime,
}
struct IntervalTreeNode {
    i:Interval,
    max:u64,
    left: Option<Box<IntervalTreeNode>>,
    right: Option<Box<IntervalTreeNode>>,
}

