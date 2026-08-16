use chrono::NaiveDateTime;

struct Interval{
    start_time:NaiveDateTime,
    end_time:NaiveDateTime,
}
impl Interval{
    fn new(start_time:NaiveDateTime, end_time:NaiveDateTime) -> Self {
        Interval{start_time, end_time}
    }
}

struct IntervalTreeNode {
    i:Interval,
    max:NaiveDateTime,
    left: Option<Box<IntervalTreeNode>>,
    right: Option<Box<IntervalTreeNode>>,

}

impl IntervalTreeNode {
    fn new(i:Interval,max:NaiveDateTime) -> Self {
        IntervalTreeNode{i,max,left:None,right:None}
    }
    fn add_left(&mut self, left:IntervalTreeNode){
        self.left = Some(Box::new(left));
    }
    fn add_right(&mut self,right:IntervalTreeNode){
        self.right = Some(Box::new(right));
    }
}
struct IntervalTree {
    root:IntervalTreeNode,
}
impl IntervalTree {
    fn new(root:IntervalTreeNode) -> Self {
        IntervalTree{root}
    }
    fn insert(&mut self, interval:Interval){
        todo!()
    }
}