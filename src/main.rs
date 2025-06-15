use std::fs;
use std::env;
use std::path::PathBuf;
use std::io::{stdout, Write};

use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor};
use crossterm::{
    event::{read, Event, KeyCode, KeyEvent, KeyModifiers},
    terminal::{disable_raw_mode, enable_raw_mode},
};

use recall::lang::scan::Scanner;
use recall::lang::parse::Parser;
use recall::lang::parse::parse_program;
use recall::eval;
use recall::storage::db::DB;
use recall::errors;

#[derive(Debug)]
struct Config {
    name: String,
    h: bool,
    program: String,
    db: Option<PathBuf>,
}

fn main() {
    let result =
        parse_config(std::env::args().peekable())
        .and_then(run);

    if let Err(err) = result {
        eprintln!("{}", err);
        std::process::exit(1);
    }
}

fn parse_config(mut iter: impl Iterator<Item = String>) -> Result<Config, errors::RecallError> {
    let name =
        iter
        .next()
        .unwrap();

    let mut result = Config {
        name, h: false, program: String::new(), db: None,
    };

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "-h" => {
                result.h = true;
            },
            "-f" => {
                if let Some(path) = iter.next() {
                    let s = fs::read_to_string(path).unwrap();
                    result.program.push_str(&s);
                } else {
                    return Err(errors::RecallError::CLIError(format!(
                        "expected program text file after '-f'",
                    )))
                }
            },
            "-s" => {
                if let Some(s) = iter.next() {
                    result.program.push_str(&s);
                } else {
                    return Err(errors::RecallError::CLIError(format!(
                        "expected program snippet after '-s'",
                    )))
                }
            },
            other if other.starts_with('-') => {
                return Err(errors::RecallError::CLIError(format!(
                    "unknown flag '{}'", other,
                )))
            },
            positional => {
                if result.db.is_none() {
                    result.db = Some(PathBuf::from(positional));
                } else {
                    return Err(errors::RecallError::CLIError(format!(
                        "extra positional argument '{}'", positional,
                    )))
                }
            },
        }
    }

    Ok(result)
}

fn run(cfg: Config) -> Result<(), errors::RecallError> {
    if cfg.h {
        println!("{}", help(&cfg.name));
        return Ok(())
    }

    let db = if let Some(file_path) = cfg.db {
        DB::new(file_path)?
    } else {
        DB::new_temp()?
    };

    if cfg.program.is_empty() {
        run_repl(db)?;
    } else {
        run_batch(&cfg.program, db)?;
    }

    Ok(())
}

fn run_batch(program: &String, db: DB) -> Result<(), errors::RecallError> {
    let scanner = Scanner::new(program);
    let mut parser = Parser::new(scanner);
    let program = parse_program(&mut parser)?;
    let txn = db.begin_transaction();
    let naive_evaluator = eval::naive::NaiveEvaluator{};
    let results = match eval::batch_eval(program, &txn, naive_evaluator) {
        Ok(results) => {
            txn.commit()?;
            results
        },
        Err(e) => {
            if let Err(e0) = txn.rollback() {
                eprintln!("error could not rollback transaction: {}", e0)
            }
            return Err(e)
        },
    };
    results
    .into_iter()
    .for_each(|term| {
        println!("{}", term);
    });

    Ok(())
}

fn run_repl(db: DB) -> Result<(), errors::RecallError> {
    let mut rl = DefaultEditor::new()?;

    println!("Welcome to the recall repl");

    let history_path: PathBuf;

    if let Ok(xdg) = env::var("XDG_CACHE_HOME") {
        history_path = PathBuf::from(xdg).join("recall").join("history");
    } else if let Ok(home) = env::var("HOME") {
        history_path = PathBuf::from(home)
            .join(".cache")
            .join("recall")
            .join("history");

    } else {
        history_path = PathBuf::from("recall_history");
    }

    if let Some(dir) = history_path.parent() {
        fs::create_dir_all(dir)?;
    }

    println!("History location: {}", history_path.to_str().unwrap_or("path not printable"));

    if rl.load_history(&history_path).is_err() {
        // no previous history
        println!("Starting new history file");
    } else {
        println!("Previous history file found");
    }

    println!("Type in a datalog statement.");

    'toplevel: loop {
        let readline = rl.readline("recall> ");
        match readline {
            Ok(line) => {
                let scanner = Scanner::new(&line);
                let mut parser = Parser::new(scanner);
                let program = match parse_program(&mut parser) {
                    Ok(program) => program,
                    Err(err) => {
                        eprintln!("{}", err);
                        continue 'toplevel
                    }
                };
                let txn = db.begin_transaction();
                let naive_evaluator = eval::naive::NaiveEvaluator{};
                let mut iter = match eval::iter_eval(program, &txn, naive_evaluator) {
                    Ok(iter) => {
                        txn.commit()?;
                        iter
                    },
                    Err(e) => {
                        if let Err(e0) = txn.rollback() {
                            eprintln!("warning: failed to rollback transaction: {}", e0);
                        }
                        eprintln!("{}", e);
                        continue 'toplevel
                    },
                };

                rl.add_history_entry(line.as_str())?;

                // iterate (possibly infinite (not at the moment)) results
                enable_raw_mode()?;

                let mut seen = false;

                'results_level: loop {
                    if let Some(term) = iter.next() {
                        if !seen {
                            print!("Press CTRL-C to exit, any other key will fetch the next result\r\n");
                            seen = true;
                        }
                        print!("{}\r\n", term);
                        stdout().flush()?;

                        // wait for a single key press
                        match read()? {
                            Event::Key(KeyEvent { code, modifiers, .. }) => {
                                match (code, modifiers) {
                                    (KeyCode::Char('c'), m) if m.contains(KeyModifiers::CONTROL) => {
                                        // Ctrl-C → break out
                                        disable_raw_mode()?;
                                        break 'results_level
                                    }
                                    (KeyCode::Char('d'), m) if m.contains(KeyModifiers::CONTROL) => {
                                        // Ctrl-C → break out
                                        // todo break outer loop
                                        disable_raw_mode()?;
                                        break 'toplevel
                                    }
                                    _ => {
                                        continue 'results_level
                                    }
                                }
                            }
                            _ => {}
                        }
                    } else {
                        disable_raw_mode()?;
                        break 'results_level
                    }
                }
            },
            Err(ReadlineError::Interrupted) => {
                // cancels current line
                continue 'toplevel
            },
            Err(ReadlineError::Eof) => {
                break 'toplevel
            },
            Err(err) => {
                return Err(err.into())
            }
        }
    }

    rl.save_history(&history_path)?;

    Ok(())
}

fn help(program: &str) -> String {
    let mut result = String::new();
    result.push_str("program path: ");
    result.push_str(program);
    result.push('\n');
    result.push_str("usage: ");
    result.push_str("<program> [ options ] [ db ]\n");
    result.push('\n');
    result.push_str("To start a repl leave out any options and (optional) supply\n");
    result.push_str("a positional argument to a database root folder. If no folder\n");
    result.push_str("is supplied it will create a fresh database just for this run.\n");
    result.push_str("Upon exiting, it will drop the database.\n");
    result.push('\n');
    result.push_str("Options:\n");
    result.push_str("-h:            help\n");
    result.push_str("-f file:       read in datalog program from file\n");
    result.push_str("-s '...':      read in datalog program from string\n");
    result.push('\n');
    result.push_str("Argument:\n");
    result.push_str("[db]:          path to db root folder, if not supplied, create\n");
    result.push_str("               a fresh database in a temp location, then drop\n");
    result.push_str("               it before the program exits.\n");
    result
}
