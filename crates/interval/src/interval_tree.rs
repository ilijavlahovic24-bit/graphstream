use chrono::NaiveDateTime;
use serde_json::Value;
use std::collections::HashMap;

#[derive(Clone,Debug,PartialEq)]
struct Interval{
    start_time:NaiveDateTime,
    end_time:NaiveDateTime,
    pub properties:HashMap<String,Value>,
}
impl Interval{
    fn new(start_time:NaiveDateTime, end_time:NaiveDateTime) -> Self {
        Interval{start_time, end_time,properties:HashMap::new()}
    }
    pub fn with_properties(mut self, properties:HashMap<String,Value>) -> Self {
        self.properties = properties;
        self
    }
    #[inline]
    pub fn overlaps(&self, t1:NaiveDateTime, t2:NaiveDateTime) -> bool {
        self.start_time <= t2 && self.end_time >= t1
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