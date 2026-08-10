use std::cell::RefCell;
use std::rc::Rc;
use chrono::NaiveDateTime;
struct Interval{
    start_time:NaiveDateTime,
    end_time:NaiveDateTime,
}
struct IntervalTreeNode {
    i:Interval,
    max:u64,
    left:RefCell<Rc<IntervalTreeNode>>,
    right:RefCell<Rc<IntervalTreeNode>>
}