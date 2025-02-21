use std::{
    iter::Peekable,
    str::Chars,
};

#[derive(Debug, Eq, PartialEq)]
pub enum Token {
    Eof,
    Ident(String),
    Var(String),
    Int(i64),
    String(String),
    Whitespace(String),
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

fn is_string(ch: &char) -> bool {
    *ch != '"'
}

fn is_start_of_var(ch: &char) -> bool {
    ch.is_uppercase() && ch.is_ascii_alphabetic() || *ch == '_'
}

fn is_var(ch: &char) -> bool {
    ch.is_ascii_alphanumeric() || *ch == '_'
}

// TODO: only supporting integer numbers
fn is_start_of_num(ch: &char) -> bool {
    ch.is_ascii_digit()
}

fn is_num(ch: &char) -> bool {
    ch.is_ascii_digit() || *ch == '_'
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

fn scan_whitespace(stream: &mut Peekable<impl Iterator<Item = char>>) -> Token {
    let value = scan_repeatedly(stream, |ch| ch.is_whitespace());

    Token::Whitespace(value)
}

fn scan_ident(stream: &mut Peekable<impl Iterator<Item = char>>) -> Token {
    let value = scan_repeatedly(stream, is_ident);

    Token::Ident(value)
}

fn scan_string(stream: &mut Peekable<impl Iterator<Item = char>>) -> Token {
    let value = scan_repeatedly(stream, is_string);

    Token::String(value)
}

fn scan_var(stream: &mut Peekable<impl Iterator<Item = char>>) -> Token {
    let value = scan_repeatedly(stream, is_var);

    Token::Var(value)
}

// TODO: fix this hacky approach
fn scan_num(
    stream: &mut Peekable<impl Iterator<Item = char>>,
    positive_sign: bool,
) -> Result<Token, String> {
    let mut value = String::from(if positive_sign { "" } else { "-" });

    value.push_str(&scan_repeatedly(stream, is_num));

    value
        .replace("_", "")
        .parse::<i64>()
        .map(|n| Token::Int(n))
        .map_err(|e| format!("Error during scanning number `{}`: {}", value, e))
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
                Ok(scan_whitespace(&mut self.stream))
            } else if is_start_of_ident(ch) {
                Ok(scan_ident(&mut self.stream))
            } else if is_start_of_string(ch) {
                self.stream.next();
                let result = scan_string(&mut self.stream);
                match self.stream.peek() {
                    Some(ch) => {
                        assert!(*ch == '"');
                        self.stream.next();
                        Ok(result)
                    }
                    None => {
                        Err(format!(
                            "Error during scanning: unexpected end of stream reached"
                        ))
                    }
                }
            } else if is_start_of_num(ch) {
                Ok(scan_num(&mut self.stream, true)?)
            } else if is_start_of_var(ch) {
                Ok(scan_var(&mut self.stream))
            // Punctuation
            } else if *ch == '(' {
                self.stream.next();
                Ok(Token::OpenParen)
            } else if *ch == ')' {
                self.stream.next();
                Ok(Token::CloseParen)
            // Operators
            } else if *ch == '.' {
                self.stream.next();
                Ok(Token::Period)
            } else if *ch == ',' {
                self.stream.next();
                Ok(Token::Comma)
            } else if *ch == '+' || *ch == '-' {
                let positive = *ch == '+';

                self.stream.next();

                if let Some(ch) = self.stream.peek() {
                    if is_start_of_num(ch) {
                        Ok(scan_num(&mut self.stream, positive)?)
                    } else {
                        return Err(format!(
                            "Error during scanning number: unexpected character: `{}`",
                            ch,
                        ));
                    }
                } else {
                    return Err(format!(
                        "Error during scanning: unexpected end of stream reached"
                    ));
                }
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
                "foo(\"bar\", 42).",
                vec![
                    Ident("foo".to_string()),
                    OpenParen,
                    String("bar".to_string()),
                    Comma,
                    Int(42),
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
            ("+", "Error during scanning: unexpected end of stream reached"),
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
