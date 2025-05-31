use std::iter::Peekable;
use std::str::Chars;

use crate::errors;

#[derive(Debug)]
pub enum DatalogToken {
    Assertion,
    Retraction,
    Query,
    If,
    Comma,
    Plus,
    Minus,
    LeftParen,
    RightParen,
    End,
    Atom(String),
    String(String),
    Var(String),
    Integer(i32),
    Float(f32),
    Expr(String), // used only by parsing, not a native datalog token
}

impl DatalogToken {
    fn from_integer_string(s: String) -> Result<Self, errors::RecallError> {
        s
        .parse::<i32>()
        .map(|n| Self::Integer(n))
        .map_err(|err| errors::RecallError::ScanError(err.to_string()))
    }

    fn from_float_string(s: String) -> Result<Self, errors::RecallError> {
        s
        .parse::<f32>()
        .map(|n| Self::Float(n))
        .map_err(|err| errors::RecallError::ScanError(err.to_string()))
    }
}

pub struct Scanner<'a> {
    stream: Peekable<Chars<'a>>,
}

impl<'a> Scanner<'a> {
    pub fn new(contents: &'a str) -> Self {
        Self {
            stream: contents.chars().peekable(),
        }
    }

    pub fn next_token(&mut self) -> Result<DatalogToken, errors::RecallError> {
        use DatalogToken::End;

        self
        .stream
        .next()
        .map_or(Ok(End), |c| self.next_token_char(c))
    }

    fn next_token_char(&mut self, mut c: char) -> Result<DatalogToken, errors::RecallError> {
        use DatalogToken as Tok;

        while c.is_whitespace() {
            match self.stream.next() {
                Some(c0) => c = c0,
                None => return Ok(Tok::End),
            }
        }

        if c == '.' { /* either an assertion or start of a decimal number */
            match self.stream.peek() {
                Some(c0) if c0.is_digit(10) => {
                    let mut result = c.to_string();
                    self.scan_digits(&mut result)?;
                    self.scan_num_suffix(&mut result)?;
                    DatalogToken::from_float_string(result)
                },
                _ => Ok(Tok::Assertion),
            }
        } else if c == '!' {
            Ok(Tok::Retraction)
        } else if c == '?' {
            Ok(Tok::Query)
        } else if c == ',' {
            Ok(Tok::Comma)
        } else if c == '(' {
            Ok(Tok::LeftParen)
        } else if c == ')' {
            Ok(Tok::RightParen)
        } else if c == '+' {
            Ok(Tok::Plus)
        } else if c == '-' {
            Ok(Tok::Minus)
        } else if c == ':' {
            match self.stream.peek() {
                Some(&'-') => {
                    self.stream.next();
                    Ok(Tok::If)
                }
                Some(c) => Err(errors::RecallError::ScanError(format!(
                    "found unexpected char {:?} when parsing",
                    c,
                ))),
                None => Err(errors::RecallError::ScanError(format!(
                    "unexpected end of input"
                ))),
            }
        } else if c.is_uppercase() && c.is_ascii_alphabetic() || c == '_' { /* start of var */
            let mut result = c.to_string();

            while let Some(c) = self.stream.peek() {
                if c.is_ascii_alphanumeric() || *c == '_' {
                    result.push(*c);
                    self.stream.next();
                } else {
                    break;
                }
            }

            Ok(Tok::Var(result))
        } else if c.is_lowercase() && c.is_ascii_alphabetic() { /* start of atom */
            let mut result = c.to_string();

            while let Some(c) = self.stream.peek() {
                if c.is_ascii_alphanumeric() || *c == '_' {
                    result.push(*c);
                    self.stream.next();
                } else {
                    break;
                }
            }

            Ok(Tok::Atom(result))
        } else if c.is_digit(10) { /* start of number */
            let mut result = c.to_string();

            if let Some(c) = self.stream.peek() {
                if c.is_digit(10) {
                    self.scan_digits(&mut result)?;
                }
            }

            match self.stream.peek() {
                Some(&'.') => {
                    result.push(self.stream.next().unwrap());

                    if let Some(c) = self.stream.peek() {
                        if c.is_digit(10) {
                            self.scan_digits(&mut result)?;
                        }
                    }

                    self.scan_num_suffix(&mut result)?;
                    DatalogToken::from_float_string(result)
                }
                Some(&'e') | Some(&'E') => {
                    self.scan_num_suffix(&mut result)?;
                    DatalogToken::from_float_string(result)
                }
                _ => DatalogToken::from_integer_string(result),
            }
        } else if c == '"' { /* start of string */
            self.scan_string(String::new())
        } else {
            Err(errors::RecallError::ScanError(format!(
                "found unexpected char {:?} when parsing",
                c,
            )))
        }
    }

    fn scan_string(&mut self, mut buf: String) -> Result<DatalogToken, errors::RecallError> {
        loop {
            match self.stream.peek() {
                Some(&'"') => {
                    self.stream.next();
                    break Ok(DatalogToken::String(buf))
                }
                Some(&'\\') => {
                    self.stream.next();
                    match self.stream.peek() {
                        Some('"') => {
                            self.stream.next();
                            buf.push('"');
                            continue
                        }
                        Some('\\') => {
                            self.stream.next();
                            buf.push('\\');
                            continue
                        }
                        Some('t') => {
                            self.stream.next();
                            buf.push('\t');
                            continue
                        }
                        Some('n') => {
                            self.stream.next();
                            buf.push('\n');
                            continue
                        }
                        Some('r') => {
                            self.stream.next();
                            buf.push('\r');
                            continue
                        }
                        Some('0') => {
                            self.stream.next();
                            buf.push('\0');
                            continue
                        }
                        Some(c) =>
                            break Err(errors::RecallError::ScanError(format!(
                                "invalid escape '\\{}'",
                                c,
                            ))),
                        None =>
                            break Err(errors::RecallError::ScanError(format!(
                                "unexpected end of input before end of string"
                            ))),
                    }
                }
                Some(_) => {
                    buf.push(self.stream.next().unwrap());
                    continue
                }
                None =>
                    break Err(errors::RecallError::ScanError(format!(
                        "unexpected end of input"
                    ))),
            }
        }
    }

    fn scan_num_suffix(&mut self, buf: &mut String) -> Result<(), errors::RecallError> {
        match self.stream.peek() {
            Some(&'e') | Some(&'E') => {
                buf.push(self.stream.next().unwrap());

                if let Some(c) = self.stream.peek() {
                    if *c == '-' || *c == '+' {
                        buf.push(self.stream.next().unwrap());
                    }
                }

                self.scan_digits(buf)?;
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn scan_digits(&mut self, buf: &mut String) -> Result<(), errors::RecallError> {
        self.scan_digit(buf)?;
        loop {
            match self.stream.peek() {
                Some(c) if c.is_digit(10) => {
                    self.scan_digit(buf)?;
                    continue
                }
                Some(&'_') => {
                    self.stream.next();
                    self.scan_digit(buf)?;
                    continue
                }
                _ => break Ok(()),
            }
        }
    }

    fn scan_digit(&mut self, buf: &mut String) -> Result<(), errors::RecallError> {
        match self.stream.peek() {
            Some(c) if c.is_digit(10) => {
                buf.push(self.stream.next().unwrap());
                Ok(())
            },
            Some(c) =>
                Err(errors::RecallError::ScanError(format!(
                    "found unexpected character {:?} when parsing number",
                    c,
                ))),
            None =>
                Err(errors::RecallError::ScanError(format!(
                    "unexpected end of input"
                ))),
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use DatalogToken as Tok;

    #[test]
    fn it_scans_valid_tokens() {
        let mut scanner = Scanner::new("foo(a, -42, 69.69).");
        assert!(matches!(scanner.next_token().unwrap(), Tok::Atom(_)));
        assert!(matches!(scanner.next_token().unwrap(), Tok::LeftParen));
        assert!(matches!(scanner.next_token().unwrap(), Tok::Atom(_)));
        assert!(matches!(scanner.next_token().unwrap(), Tok::Comma));
        assert!(matches!(scanner.next_token().unwrap(), Tok::Minus));
        assert!(matches!(scanner.next_token().unwrap(), Tok::Integer(_)));
        assert!(matches!(scanner.next_token().unwrap(), Tok::Comma));
        assert!(matches!(scanner.next_token().unwrap(), Tok::Float(_)));
        assert!(matches!(scanner.next_token().unwrap(), Tok::RightParen));
        assert!(matches!(scanner.next_token().unwrap(), Tok::Assertion));
        assert!(matches!(scanner.next_token().unwrap(), Tok::End));

        let mut scanner = Scanner::new("foo(Bar123)?");
        assert!(matches!(scanner.next_token().unwrap(), Tok::Atom(_)));
        assert!(matches!(scanner.next_token().unwrap(), Tok::LeftParen));
        assert!(matches!(scanner.next_token().unwrap(), Tok::Var(_)));
        assert!(matches!(scanner.next_token().unwrap(), Tok::RightParen));
        assert!(matches!(scanner.next_token().unwrap(), Tok::Query));
        assert!(matches!(scanner.next_token().unwrap(), Tok::End));

        let mut scanner = Scanner::new("foo(X) :- true, bar(X, _)!");
        assert!(matches!(scanner.next_token().unwrap(), Tok::Atom(_)));
        assert!(matches!(scanner.next_token().unwrap(), Tok::LeftParen));
        assert!(matches!(scanner.next_token().unwrap(), Tok::Var(_)));
        assert!(matches!(scanner.next_token().unwrap(), Tok::RightParen));
        assert!(matches!(scanner.next_token().unwrap(), Tok::If));
        assert!(matches!(scanner.next_token().unwrap(), Tok::Atom(_)));
        assert!(matches!(scanner.next_token().unwrap(), Tok::Comma));
        assert!(matches!(scanner.next_token().unwrap(), Tok::Atom(_)));
        assert!(matches!(scanner.next_token().unwrap(), Tok::LeftParen));
        assert!(matches!(scanner.next_token().unwrap(), Tok::Var(_)));
        assert!(matches!(scanner.next_token().unwrap(), Tok::Comma));
        assert!(matches!(scanner.next_token().unwrap(), Tok::Var(_)));
        assert!(matches!(scanner.next_token().unwrap(), Tok::RightParen));
        assert!(matches!(scanner.next_token().unwrap(), Tok::Retraction));
        assert!(matches!(scanner.next_token().unwrap(), Tok::End));
    }

    #[test]
    fn it_matches_atom_payload() {
        let mut scanner = Scanner::new("foo");
        match scanner.next_token().unwrap() {
            Tok::Atom(s) => assert_eq!(s, "foo"),
            _ => panic!("did not match"),
        }
    }

    #[test]
    fn it_matches_var_payload() {
        let mut scanner = Scanner::new("Bar");
        match scanner.next_token().unwrap() {
            Tok::Var(s) => assert_eq!(s, "Bar"),
            _ => panic!("did not match"),
        }
    }

    #[test]
    fn it_matches_string_payload() {
        for (input, want) in [
            ("\"Hello, world!\"", "Hello, world!"),
            ("\"The \\\"Great\\\" Whale\"", "The \"Great\" Whale"),
            ("\"line1\n\"", "line1\n" ),
        ] {
            let mut scanner = Scanner::new(input);
            match scanner.next_token().unwrap() {
                Tok::String(got) => assert_eq!(got, want),
                _ => panic!("did not match"),
            }
        }
    }

    #[test]
    fn it_matches_float_payload() {
        for (input, want) in [
            ("1.", "1"),
            ("0.1", "0.1"),
            ("1.1", "1.1"),
            (".1", "0.1"),
            (".1", "0.1"),
            ("0.1", "0.1"),
            (".1e1", "1"),
            ("1e1", "10"),
            ("1.1e1", "11"),
            ("42e1", "420"),
            (".1e-1", "0.01"),
            ("1e-1", "0.1"),
            ("1.1e-1", "0.11"),
            ("42e-1", "4.2"),
            ("42e-1", "4.2"),
            ("42.", "42"),
        ] {
            let mut scanner = Scanner::new(input);
            match scanner.next_token().unwrap() {
                Tok::Float(got) => assert_eq!(got.to_string(), want),
                // Tok::Float(got) => println!("{:?}", got.to_string()),
                _ => panic!("did not match"),
            }
        }
    }

    #[test]
    fn it_matches_integer_payload() {
        let mut scanner = Scanner::new("42");
        match scanner.next_token().unwrap() {
            Tok::Integer(s) => assert_eq!(s, 42),
            _ => panic!("did not match"),
        }
    }

    #[test]
    fn it_reports_errors_on_invalid_input() {
        let mut scanner = Scanner::new("*");
        assert!(scanner.next_token().is_err());

        let mut scanner = Scanner::new(":");
        assert!(scanner.next_token().is_err());

        let mut scanner = Scanner::new("^");
        assert!(scanner.next_token().is_err());
    }

    #[test]
    fn it_ignores_whitespace() {
        let mut scanner = Scanner::new("    foo   (\ta,b\t) .");
        assert!(matches!(scanner.next_token().unwrap(), Tok::Atom(_)));
        assert!(matches!(scanner.next_token().unwrap(), Tok::LeftParen));
        assert!(matches!(scanner.next_token().unwrap(), Tok::Atom(_)));
        assert!(matches!(scanner.next_token().unwrap(), Tok::Comma));
        assert!(matches!(scanner.next_token().unwrap(), Tok::Atom(_)));
        assert!(matches!(scanner.next_token().unwrap(), Tok::RightParen));
        assert!(matches!(scanner.next_token().unwrap(), Tok::Assertion));
        assert!(matches!(scanner.next_token().unwrap(), Tok::End));
    }
}
