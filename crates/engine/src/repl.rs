use std::sync::Arc;

use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use rustyline::Editor;

use query::{QueryEngine, QueryResult, ResultCell, TqlError};
use temporal_graph::TemporalGraph;

/// Interactive TQL REPL.
pub struct Repl {
    engine: Arc<QueryEngine>,
}

impl Repl {
    pub fn new(graph: Arc<TemporalGraph>) -> Self {
        Self { engine: Arc::new(QueryEngine::new(graph)) }
    }

    /// Run the REPL until `:exit`, `:quit`, or EOF.
    pub fn run(&mut self) -> anyhow::Result<()> {
        let mut rl: Editor<(), DefaultHistory> = Editor::new()?;
        let _ = rl.load_history(".graphstream_history");

        self.print_banner();

        let mut buffer = String::new();
        let mut prompt = "tql> ";

        loop {
            match rl.readline(prompt) {
                Ok(line) => {
                    let trimmed = line.trim();

                    // Meta-commands are only recognized when the buffer is empty.
                    if buffer.is_empty() && trimmed.starts_with(':') {
                        match self.handle_meta(trimmed, &mut rl) {
                            MetaResult::Continue => { prompt = "tql> "; continue; }
                            MetaResult::Exit => break,
                        }
                    }

                    let _ = rl.add_history_entry(trimmed);

                    if buffer.is_empty() {
                        buffer.push_str(trimmed);
                    } else {
                        buffer.push('\n');
                        buffer.push_str(trimmed);
                    }

                    // A query is complete when the line ends with `;` or the line is empty.
                    let complete = trimmed.ends_with(';') || trimmed.is_empty();
                    if !complete {
                        prompt = "  -> ";
                        continue;
                    }

                    // Strip the trailing `;` before sending the query to the parser.
                    let tql = buffer.trim_end_matches(';').trim().to_string();
                    buffer.clear();
                    prompt = "tql> ";

                    if tql.is_empty() { continue; }

                    match self.engine.query(&tql) {
                        Ok(res) => print_result(&res),
                        Err(e) => print_error(&e),
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    // Ctrl+C — cancel the current input.
                    buffer.clear();
                    prompt = "tql> ";
                    println!("^C");
                }
                Err(ReadlineError::Eof) => break,
                Err(e) => {
                    eprintln!("REPL error: {e}");
                    break;
                }
            }
        }

        let _ = rl.save_history(".graphstream_history");
        Ok(())
    }

    fn handle_meta(&mut self, line: &str, rl: &mut Editor<(), DefaultHistory>) -> MetaResult {
        let mut parts = line.splitn(2, char::is_whitespace);
        let cmd = parts.next().unwrap_or("");
        let arg = parts.next().map(|s| s.trim());

        match cmd {
            ":help" | ":h" => {
                println!(
                    "Meta-commands:\n  \
                     :help, :h            show this help\n  \
                     :exit, :quit, :q     exit the REPL\n  \
                     :load <file>         execute a .tql file\n  \
                     :clear               clear the screen\n  \
                     :history             show command history\n\
                     \n\
                     Terminate a query with `;` or an empty line.\n\
                     Ctrl+C cancels the current input, Ctrl+D exits."
                );
            }
            ":exit" | ":quit" | ":q" => return MetaResult::Exit,
            ":clear" => {
                print!("\x1b[2J\x1b[H");
            }
            ":history" => {
                for (i, h) in rl.history().iter().enumerate() {
                    println!("{:>4}  {}", i + 1, h);
                }
            }
            ":load" => {
                let Some(path) = arg else {
                    eprintln!("usage: :load <file.tql>");
                    return MetaResult::Continue;
                };
                match std::fs::read_to_string(path) {
                    Ok(src) => {
                        let tql = src.trim_end_matches(';').trim();
                        match self.engine.query(tql) {
                            Ok(res) => print_result(&res),
                            Err(e) => print_error(&e),
                        }
                    }
                    Err(e) => eprintln!("cannot read `{path}`: {e}"),
                }
            }
            other => eprintln!("unknown command: {other}  (try :help)"),
        }
        MetaResult::Continue
    }

    fn print_banner(&self) {
        println!(
            "GraphStream TQL REPL\n\
             Type a query and end it with `;`, or `:help` for help, `:exit` to quit.\n"
        );
    }
}

enum MetaResult { Continue, Exit }

// ---- output formatting ------------------------------------------------

fn print_result(res: &QueryResult) {
    if res.rows.is_empty() {
        println!("(0 rows)");
        return;
    }

    // Compute column widths.
    let mut widths: Vec<usize> = res.columns.iter().map(|c| c.len()).collect();
    for row in &res.rows {
        for (i, cell) in row.cells.iter().enumerate() {
            let w = format_cell(cell).len();
            if w > widths[i] { widths[i] = w; }
        }
    }

    // Header.
    let header = res.columns.iter().enumerate()
        .map(|(i, c)| format!("{:<width$}", c, width = widths[i]))
        .collect::<Vec<_>>()
        .join(" | ");
    println!("{header}");
    println!(
        "{}",
        widths.iter().map(|w| "-".repeat(*w)).collect::<Vec<_>>().join("-+-")
    );

    // Rows.
    for row in &res.rows {
        let line = row.cells.iter().enumerate()
            .map(|(i, c)| format!("{:<width$}", format_cell(c), width = widths[i]))
            .collect::<Vec<_>>()
            .join(" | ");
        println!("{line}");
    }

    println!("\n({} {})", res.rows.len(),
             if res.rows.len() == 1 { "row" } else { "rows" });
}

fn format_cell(cell: &ResultCell) -> String {
    match cell {
        ResultCell::Null => "null".into(),
        ResultCell::Value(v) => v.to_string(),
        ResultCell::Node { id, label, .. } => format!("{label}#{}", id.0),
        ResultCell::Edge { id, label, .. } => format!("{label}#{}", id.0),
    }
}

fn print_error(e: &TqlError) {
    eprintln!("error: {e}");
    if let TqlError::Parse(pe) = e {
        eprintln!("  at {}:{}", pe.line, pe.column);
        if let Some(expected) = &pe.expected {
            eprintln!("  expected: {expected}");
        }
    }
}