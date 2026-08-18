# TQL Grammar Specification

Temporal Query Language (TQL) - EBNF grammar for GraphStream v1.

## Notation

| Symbol | Meaning |
|---|---|
| `=` | definition |
| `,` | concatenation |
| `\|` | alternation |
| `[ ]` | optional |
| `{ }` | zero or more repetitions |
| `( )` | grouping |
| `" "` | terminal string |

---

## Top-Level Query

```ebnf
query
    = match_clause ,
      [ where_clause ] ,
      [ window_clause , [ having_clause ] ] ,
      return_clause
    ;
```

---

## MATCH Clause

Cypher-style graph pattern matching.

```ebnf
match_clause
    = "MATCH" , path_pattern , { "," , path_pattern }
    ;

path_pattern
    = node_pattern , { edge_pattern , node_pattern }
    ;

node_pattern
    = "(" , binding , [ ":" , label ] , ")"
    ;

edge_pattern
    = "-[" , binding , [ ":" , label ] , "]->"   (* directed right *)
    | "<-[" , binding , [ ":" , label ] , "]-"   (* directed left  *)
    | "-[" , binding , [ ":" , label ] , "]-"    (* undirected     *)
    ;

binding = identifier ;
label   = identifier ;
```

---

## WHERE Clause

```ebnf
where_clause
    = "WHERE" , condition , { "AND" , condition }
    ;

condition
    = temporal_condition
    | property_condition
    ;
```

### Temporal Conditions

```ebnf
temporal_condition
    = at_condition
    | between_condition
    | during_condition
    | ordering_condition
    | within_condition
    | diff_condition
    ;

at_condition
    = "AT" , "t" , "=" , time_expr
    ;

between_condition
    = time_expr , "BETWEEN" , time_expr , "," , time_expr
    ;

during_condition
    = time_expr , "DURING" , "[" , time_expr , "," , time_expr , "]"
    ;

ordering_condition
    = time_expr , ( "BEFORE" | "AFTER" ) , time_expr
    ;

within_condition
    = "WITHIN" , "(" , time_expr , "," , time_expr , ")" ,
      comparison_op , duration_literal
    ;

diff_condition
    = "DIFF" , "(" , property_ref , "," , time_expr , "," , time_expr , ")" ,
      comparison_op , value
    ;
```

### Property Conditions

```ebnf
property_condition
    = property_ref , comparison_op , value
    | property_ref , "!=" , property_ref
    ;

property_ref  = binding , "." , identifier ;
comparison_op = "=" | "!=" | "<" | "<=" | ">" | ">=" ;

value
    = number_literal
    | string_literal
    | bool_literal
    | "null"
    ;
```

---

## WINDOW Clause

```ebnf
window_clause
    = "WINDOW" , duration_literal , [ window_type ]
    ;

window_type
    = "SLIDING"
    | "TUMBLING"
    ;
```

> Default window type is `TUMBLING` when not specified.

---

## HAVING Clause

Only valid when `WINDOW` is present.

```ebnf
having_clause
    = "HAVING" , aggregate_expr , comparison_op , number_literal
    ;

aggregate_expr
    = "COUNT" , "(" , ( "*" | property_ref ) , ")"
    | "SUM"   , "(" , property_ref , ")"
    | "AVG"   , "(" , property_ref , ")"
    | "MIN"   , "(" , property_ref , ")"
    | "MAX"   , "(" , property_ref , ")"
    ;
```

---

## RETURN Clause

```ebnf
return_clause
    = "RETURN" , return_item , { "," , return_item }
    ;

return_item
    = binding                              (* node or edge *)
    | property_ref                         (* single property *)
    | aggregate_expr , "AS" , identifier   (* named aggregation *)
    ;
```

---

## Time Expressions and Duration Literals

```ebnf
time_expr
    = property_ref      (* e.g. r1.time, parent.time *)
    | timestamp_literal (* absolute Unix epoch milliseconds *)
    | identifier        (* named variable, e.g. t, t1, t2 *)
    ;

timestamp_literal = integer_literal ;

duration_literal
    = number_literal , time_unit
    ;

time_unit
    = "MILLISECONDS"
    | "SECONDS"
    | "MINUTES"
    | "HOURS"
    | "DAYS"
    ;
```

> Scientific notation is supported in `number_literal` for sub-millisecond durations,
> e.g. `1.5e-12 SECONDS` for particle physics use cases.

---

## Literals and Primitives

```ebnf
number_literal
    = integer_literal
    | float_literal
    | scientific_literal
    ;

integer_literal    = digit , { digit } ;
float_literal      = digit , { digit } , "." , digit , { digit } ;
scientific_literal = ( integer_literal | float_literal ) ,
                     ( "e" | "E" ) , [ "+" | "-" ] , digit , { digit } ;

string_literal = "'" , { character } , "'"
               | '"' , { character } , '"' ;

bool_literal = "true" | "false" ;

identifier = letter , { letter | digit | "_" } ;
```

---

## Validation Examples

The following queries must parse successfully against this grammar
before parser implementation begins (per ADR-002, ADR-006).

### 7.1 Cybersecurity - Lateral Movement Detection

```
MATCH (a:Host)-[r1:Connection]->(b:Host)-[r2:Connection]->(c:Host)
WHERE r1.time BEFORE r2.time
  AND WITHIN(r1.time, r2.time) < 30 MINUTES
  AND c.critical = true
RETURN a, b, c
```

### 7.2 Finance -Coordinated Trading Detection

```
MATCH (t1:Trade)-[c:CORRELATED]->(t2:Trade)
WHERE c.correlation > 0.8
  AND t1.exchange != t2.exchange
  AND WITHIN(t1.time, t2.time) < 100 MILLISECONDS
WINDOW 1 SECOND
HAVING COUNT(*) >= 5
RETURN t1, t2
```

### 7.3 Physics - Particle Decay Chain Analysis

```
MATCH (parent:Particle)-[d:DECAYS_TO]->(child:Particle)
WHERE WITHIN(parent.time, child.time) < 1.5e-12 SECONDS
RETURN parent, SUM(child.energy) AS total_energy
```

### Snapshot Query (AT)

```
MATCH (a:Host)-[r:Connection]->(b:Host)
WHERE r.time AT t=1000
RETURN a, b
```

### Range Query (BETWEEN)

```
MATCH (a:Host)
WHERE a.created BETWEEN 1000, 2000
RETURN a
```

### Overlap Query (DURING)

```
MATCH (a:Host)-[r:Connection]->(b:Host)
WHERE r.time DURING [1000, 2000]
RETURN a, r, b
```

### State Change Query (DIFF)

```
MATCH (a:Host)
WHERE DIFF(a.status, t1, t2) != null
RETURN a
```

### Evolve Query (DIFF with aggregation)

```
MATCH (sensor:Device)-[r:Reading]->(hub:Hub)
WHERE DIFF(sensor.value, 1000, 2000) > 0
RETURN sensor, SUM(r.delta) AS total_change
```
