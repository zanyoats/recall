use std::{
    iter::Peekable,
    str::Chars,
};

#[derive(Debug, Eq, PartialEq)]
pub enum Token {
    Eof,
    Ident(String),
    Var(String),
    Number(String),
    String(String),
    Whitespace(char),
    OpenParen,
    CloseParen,
    Period,
    Comma,
    If,
}

// Conider: Making TokenKind and Token
// e.g. { token: TokenKind, text: String, span: (1,3) }

fn is_start_of_ident(ch: &char) -> bool {
    ch.is_lowercase() && ch.is_ascii_alphanumeric()
}

fn is_ident(ch: &char) -> bool {
    ch.is_lowercase() && ch.is_ascii_alphanumeric() || *ch == '_'
}

fn is_start_of_string(ch: &char) -> bool {
    *ch == '"'
}

fn is_start_of_var(ch: &char) -> bool {
    ch.is_uppercase() && ch.is_ascii_alphabetic() || *ch == '_'
}

fn is_var(ch: &char) -> bool {
    ch.is_ascii_alphanumeric() || *ch == '_'
}

fn is_start_of_num(ch: &char) -> bool {
       ch.is_digit(10)
    // || *ch == '.'
    || *ch == '+'
    || *ch == '-'
}

fn scan_repeatedly<F>(stream: &mut Peekable<impl Iterator<Item = char>>, predicate: F) -> String
where
    F: Fn(&char) -> bool,
{
    let mut value = String::new();

    while let Some(&ch) = stream.peek() {
        if predicate(&ch) {
            value.push(ch);
            stream.next();
        } else {
            break;
        }
    }

    value
}

fn scan_ident(stream: &mut Peekable<impl Iterator<Item = char>>) -> Token {
    let value = scan_repeatedly(stream, is_ident);

    Token::Ident(value)
}

fn scan_string(stream: &mut Peekable<impl Iterator<Item = char>>) -> Result<Token, String> {
    let mut val = String::new();

    loop {
        match stream.peek() {
            Some('"') => {
                stream.next();
                break Ok(Token::String(val))
            },
            Some('\\') => {
                stream.next();
                match stream.peek() {
                    Some('"') => {
                        stream.next();
                        val.push('"');
                        continue
                    }
                    Some('\\') => {
                        stream.next();
                        val.push('\\');
                        continue
                    }
                    Some('t') => {
                        stream.next();
                        val.push('\t');
                        continue
                    }
                    Some('n') => {
                        stream.next();
                        val.push('\n');
                        continue
                    }
                    Some('r') => {
                        stream.next();
                        val.push('\r');
                        continue
                    }
                    Some('0') => {
                        stream.next();
                        val.push('\0');
                        continue
                    }
                    Some(ch) => {
                        break Err(format!("Error during scanning string: invalid escape '\\{ch}'"))
                    }
                    None => {
                        break Err(format!("Error during scanning string: unexpected end of input"))
                    }
                }
            }
            Some(_) => {
                val.push(stream.next().unwrap());
                continue
            }
            None => {
                break Err(format!("Error during scanning string: unexpected end of input"))
            }
        }
    }
}

fn scan_var(stream: &mut Peekable<impl Iterator<Item = char>>) -> Token {
    let value = scan_repeatedly(stream, is_var);

    Token::Var(value)
}

fn scan_digit(
    stream: &mut Peekable<impl Iterator<Item = char>>,
    buf: &mut String,
) -> Result<(), String> {
    match stream.peek() {
        Some(ch) if ch.is_digit(10) => {
            buf.push(stream.next().unwrap());
            Ok(())
        }
        Some(bad_ch) => {
            Err(format!("Error during scanning number: unexpected character '{}'", *bad_ch))
        }
        None => {
            Err(format!("Error during scanning number: unexpected end of stream"))
        }
    }
}

fn scan_digits(
    stream: &mut Peekable<impl Iterator<Item = char>>,
    buf: &mut String,
) -> Result<(), String> {
    scan_digit(stream, buf)?;
    loop {
        match stream.peek() {
            Some(&ch) if ch.is_digit(10) => {
                scan_digit(stream, buf)?;
                continue
            }
            Some('_') => {
                stream.next();
                scan_digit(stream, buf)?;
                continue
            }
            _ => break Ok(()),
        }
    }
}

fn scan_decimal_num(
    stream: &mut Peekable<impl Iterator<Item = char>>,
) -> Result<String, String> {
    let mut val = String::from("0.");
    match stream.peek() {
        Some(&ch) if ch.is_digit(10) => {
            scan_digits(stream, &mut val)?;
            match stream.peek() {
                Some('e') => {
                    val.push(stream.next().unwrap());
                    scan_digits(stream, &mut val)?;
                    Ok(val)
                }
                _ => Ok(val),
            }
        }
        Some('e') => {
            val.push(stream.next().unwrap());
            scan_digits(stream, &mut val)?;
            Ok(val)
        }
        Some(bad_ch) => {
            Err(format!("Error during scanning number: unexpected character '{}'", *bad_ch))
        }
        None => {
            Err(format!("Error during scanning number: unexpected end of stream"))
        }
    }
}

fn scan_num(
    stream: &mut Peekable<impl Iterator<Item = char>>,
) -> Result<Token, String> {
    let mut val = String::new();

    loop {
        match stream.peek() {
            Some(&ch) if ch.is_digit(10) => {
                scan_digits(stream, &mut val)?;
                match stream.peek() {
                    Some('.') => {
                        val.push(stream.next().unwrap());
                        match stream.peek() {
                            Some(&ch) if ch.is_digit(10) => {
                                scan_digits(stream, &mut val)?;
                                match stream.peek() {
                                    Some('e') => {
                                        val.push(stream.next().unwrap());
                                        scan_digits(stream, &mut val)?;
                                        break Ok(Token::Number(val))
                                    }
                                    _ => break Ok(Token::Number(val)),
                                }
                            }
                            Some('e') => {
                                val.push(stream.next().unwrap());
                                scan_digits(stream, &mut val)?;
                                break Ok(Token::Number(val))
                            }
                            _ => break Ok(Token::Number(val)),
                        }
                    },
                    Some('e') => {
                        val.push(stream.next().unwrap());
                        scan_digits(stream, &mut val)?;
                        break Ok(Token::Number(val))
                    },
                    _ => break Ok(Token::Number(val)),
                }
            },
            Some('.') => {
                stream.next();
                let decimal_part = scan_decimal_num(stream)?;
                val.push_str(&decimal_part);
                break Ok(Token::Number(val))
            }
            Some(&ch) if ch == '+' || ch == '-' => {
                if stream.next().unwrap() == '-' {
                    val.push('-');
                }
                continue
            }
            Some(bad_ch) => {
                break Err(format!("Error during scanning number: unexpected character '{}'", *bad_ch))
            }
            None => {
                break Err(format!("Error during scanning number: unexpected end of stream"))
            }
        }
    }
}

pub struct Scanner<'a> {
    stream: Peekable<Chars<'a>>,
    buffer: Option<Token>,
}

impl<'a> Scanner<'a> {
    pub fn new(contents: &'a str) -> Self {
        Self {
            stream: contents.chars().peekable(),
            buffer: None,
        }
    }

    pub fn put_back(&mut self, token: Token) {
        self.buffer = Some(token);
    }

    pub fn next_token(&mut self) -> Result<Token, String> {
        self
            .buffer
            .take()
            .map_or_else(
                || loop {
                    match self.scan_token()? {
                        Token::Whitespace(_) => continue,
                        token => break Ok(token),
                    }
                },
                |v| Ok(v),
            )
    }

    fn scan_token(&mut self) -> Result<Token, String> {
        if let Some(ch) = self.stream.peek() {
            if ch.is_whitespace() {
                Ok(Token::Whitespace(self.stream.next().unwrap()))
            // Operators
            } else if *ch == '.' {
                self.stream.next();
                match self.stream.peek() {
                    Some(ch) if ch.is_digit(10) => {
                        let decimal_part = scan_decimal_num(&mut self.stream)?;
                        Ok(Token::Number(decimal_part))
                    },
                    _ => Ok(Token::Period)
                }
            } else if *ch == ',' {
                self.stream.next();
                Ok(Token::Comma)
            } else if *ch == ':' {
                self.stream.next();

                if let Some(&'-') = self.stream.peek() {
                    self.stream.next();
                    Ok(Token::If)
                } else {
                    return Err(format!(
                        "Error during scanning: unexpected end of stream reached"
                    ));
                }
            } else if is_start_of_ident(ch) {
                Ok(scan_ident(&mut self.stream))
            } else if is_start_of_string(ch) {
                self.stream.next();
                Ok(scan_string(&mut self.stream)?)
            } else if is_start_of_num(ch) {
                scan_num(&mut self.stream)
            } else if is_start_of_var(ch) {
                Ok(scan_var(&mut self.stream))
            // Punctuation
            } else if *ch == '(' {
                self.stream.next();
                Ok(Token::OpenParen)
            } else if *ch == ')' {
                self.stream.next();
                Ok(Token::CloseParen)
            } else {
                return Err(format!(
                    "Error during scanning: unexpected character: `{}`",
                    ch,
                ));
            }
        } else {
            Ok(Token::Eof)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use Token::*;

    macro_rules! assert_tokens {
        ($scanner:expr, $expected:expr) => {
            assert_eq!(collect_tokens(&mut $scanner), $expected);
        };
    }

    macro_rules! scan_token {
        ($scanner:expr, $expected_token:expr) => {
            assert_eq!(
                $scanner.next_token().unwrap(),
                $expected_token,
            );
        };
    }

    fn collect_tokens(scanner: &mut Scanner) -> Vec<Token> {
        let mut result = vec![];
        loop {
            match scanner.next_token() {
                Ok(Token::Eof) => break result,
                Ok(token) => {
                    result.push(token);
                    continue
                }
                Err(err) => panic!("Error during collecting tokens: {}", err),
            }
        }
    }

    #[test]
    fn it_scans_valid_tokens() {
        let test_cases = [
            (
                "foo(a, b).",
                vec![
                    Ident("foo".to_string()),
                    OpenParen,
                    Ident("a".to_string()),
                    Comma,
                    Ident("b".to_string()),
                    CloseParen,
                    Period,
                ],
            ),
            (
                "Bar123 :- foo.",
                vec![
                    Var("Bar123".to_string()),
                    If,
                    Ident("foo".to_string()),
                    Period,
                ],
            ),
            (
                r#"foo("bar\t\"hello\"\nlemon 'foo'", 42.42, .42, 42., -.42, 42e42, -42.0e42, -42.e42, .42e42, 69_420)."#,
                vec![
                    Ident("foo".to_string()),
                    OpenParen,
                    String("bar\t\"hello\"\nlemon 'foo'".to_string()),
                    Comma,
                    Number("42.42".to_string()),
                    Comma,
                    Number("0.42".to_string()),
                    Comma,
                    Number("42.".to_string()),
                    Comma,
                    Number("-0.42".to_string()),
                    Comma,
                    Number("42e42".to_string()),
                    Comma,
                    Number("-42.0e42".to_string()),
                    Comma,
                    Number("-42.e42".to_string()),
                    Comma,
                    Number("0.42e42".to_string()),
                    Comma,
                    Number("69420".to_string()),
                    CloseParen,
                    Period,
                ]
            )
        ];

        for (expr, expected_tokens) in test_cases {
            let mut scanner = Scanner::new(expr);
            assert_tokens!(scanner, expected_tokens);
        }
    }

    #[test]
    fn it_reports_errors_on_invalid_input() {
        let test_cases = [
            ("*", "Error during scanning: unexpected character: `*`"),
            (":", "Error during scanning: unexpected end of stream reached"),
            ("+", "Error during scanning number: unexpected end of stream"),
        ];

        for (expr, expected_error) in test_cases {
            let mut scanner = Scanner::new(expr);
            match scanner.next_token() {
                Ok(_) => panic!("expected to scan error"),
                Err(e) => assert_eq!(e, expected_error),
            }
        }
    }

    #[test]
    fn it_ignores_whitespace() {
        let expr = "   foo   \t(a,  \n b).\n ";
        let mut scanner = Scanner::new(expr);
        assert_tokens!(
            scanner,
            vec![
                Ident("foo".to_string()),
                OpenParen,
                Ident("a".to_string()),
                Comma,
                Ident("b".to_string()),
                CloseParen,
                Period,
            ]
        );
    }

    #[test]
    fn it_puts_back_a_token() {
        let expr = "functor.";
        let mut scanner = Scanner::new(expr);
        scan_token!(scanner, Ident("functor".to_string()));
        scan_token!(scanner, Period);
        // put back is useful here for reading in 'functor(' or 'functor.'
        scanner.put_back(Period);
        scan_token!(scanner, Period);
        scan_token!(scanner, Eof);
    }
}
