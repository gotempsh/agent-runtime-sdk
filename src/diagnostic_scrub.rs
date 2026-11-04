//! Best-effort credential scrubbing for untrusted launcher diagnostics.
//! Unknown opaque values without a credential marker cannot be recognized.
use base64::Engine;
use std::sync::LazyLock;

static PATTERNS: LazyLock<Vec<regex::Regex>> = LazyLock::new(|| {
    [
        r"(?im)(?:proxy-)?authorization\s*[=:]\s*[^\r\n]+",
        r#"(?i)\b(?:bearer|basic)\s+[^\s\"'<>]+"#,
        r#"(?i)\b[a-z][a-z0-9+.-]*://[^\s/?#\"'<>]*@[^\s\"'<>]*"#,
        r#"(?i)(?P<credential>\b[a-z][a-z0-9+.-]*://[^\s/:?#\"'<>]+:[^\s/?#\"'<>]+)(?:$|[\s\"'<>])"#,
        r#"(?ims)\b(?:[a-z0-9_]*[_-])?(?:api[_-]?key|access[_-]?token|refresh[_-]?token|token|secret(?:[_-](?:access[_-])?key)?|password|passwd|credential|client[_-]?secret|signature|private[_-]?key|(?:encryption|signing|auth)[_-]?key)\s*[\"']?\s*[:=]\s*(?:\"(?:\\.|[^\"\\])*\"|'(?:\\.|[^'\\])*'|\"(?:\\[^\r\n]|[^\"\\\r\n])*(?:\"|\\?\r?$)|'(?:\\[^\r\n]|[^'\\\r\n])*(?:'|\\?\r?$)|[^\s&,;\"'<>]+)"#,
        r"\b(?:sk-|sk_|gh[pousr]_|github_pat_|xox[baprs]-|AKIA|ASIA|eyJ)[A-Za-z0-9_./+=%-]*",
    ].into_iter().map(|p| regex::Regex::new(p).expect("credential pattern")).collect()
});

// Extract quoted diagnostic values, including cut-off lines, as well as tokens.
// Keep their original escaped bytes so matches map directly to the input.
static QUOTED_VALUES: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r#"(?s)"((?:\\.|[^"\\])*)"|'((?:\\.|[^'\\])*)'"#).expect("quoted values")
});
static UNFINISHED_QUOTED_VALUES: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r#"(?m)"((?:\\[^\r\n]|[^"\\\r\n])*)(?:"|\\?\r?$)|'((?:\\[^\r\n]|[^'\\\r\n])*)(?:'|\\?\r?$)"#).expect("quoted values")
});

pub(crate) fn scrub<'a>(text: &str, secrets: impl IntoIterator<Item = &'a str>) -> String {
    let mut values: Vec<String> = secrets
        .into_iter()
        .filter(|s| !s.is_empty())
        .flat_map(|s| {
            [
                s.to_owned(),
                s.trim().to_owned(),
                base64::engine::general_purpose::STANDARD.encode(s),
                base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(s),
                serde_json::to_string(s)
                    .unwrap_or_default()
                    .trim_matches('"')
                    .to_owned(),
            ]
        })
        .filter(|s| !s.is_empty())
        .collect();
    values.sort_unstable_by_key(|s| std::cmp::Reverse(s.len()));
    values.dedup();
    // Inspect original and decoded views without mutating either. Map decoded
    // matches back to original byte ranges so normalization and overlapping
    // values cannot hide one another, and useful text stays byte-for-byte intact.
    let mut ranges = credential_ranges(text, &values);
    let mut bytes = Vec::with_capacity(text.len());
    let mut offsets = Vec::with_capacity(text.len() + 1);
    let input = text.as_bytes();
    let mut i = 0;
    while i < input.len() {
        offsets.push(i);
        if input[i] == b'%' && i + 2 < input.len() {
            if let Ok(hex) = std::str::from_utf8(&input[i + 1..i + 3]) {
                if let Ok(byte) = u8::from_str_radix(hex, 16) {
                    bytes.push(byte);
                    i += 3;
                    continue;
                }
            }
        }
        bytes.push(input[i]);
        i += 1;
    }
    offsets.push(text.len());
    let Ok(decoded) = std::str::from_utf8(&bytes) else {
        return "Diagnostic contained invalid encoded text; content withheld to protect credentials.".into();
    };
    ranges.extend(
        credential_ranges(decoded, &values)
            .into_iter()
            .map(|(start, end)| (offsets[start], offsets[end])),
    );
    ranges.sort_unstable();
    let mut merged: Vec<(usize, usize)> = Vec::new();
    for (start, end) in ranges {
        if let Some(last) = merged.last_mut().filter(|last| start <= last.1) {
            last.1 = last.1.max(end);
        } else {
            merged.push((start, end));
        }
    }
    let mut output = String::new();
    let mut cursor = 0;
    for (start, end) in merged {
        output.push_str(&text[cursor..start]);
        output.push_str("[REDACTED]");
        cursor = end;
    }
    output.push_str(&text[cursor..]);
    output
}

fn credential_ranges(text: &str, values: &[String]) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    let mut fragments: Vec<(&str, &str)> = text
        .split_whitespace()
        .map(|token| token.trim_matches(|c: char| "\"'(),;[]".contains(c)))
        .chain(
            [&*QUOTED_VALUES, &*UNFINISHED_QUOTED_VALUES]
                .into_iter()
                .flat_map(|pattern| pattern.captures_iter(text))
                .filter_map(|quoted| quoted.get(1).or_else(|| quoted.get(2)))
                .map(|quoted| quoted.as_str()),
        )
        .flat_map(|token| {
            // Compare the credential bytes, but redact the original fragment,
            // including a diagnostic field label and any truncation marker.
            // Keep candidate fan-out bounded, independent of input length.
            std::iter::once(token)
                .chain(token.split_once(':').map(|(_, value)| value))
                .chain(token.split_once('=').map(|(_, value)| value))
                .map(move |value| {
                    let value = value
                        .strip_suffix("...")
                        .or_else(|| value.strip_suffix('…'))
                        .unwrap_or(value);
                    (token, value)
                })
        })
        .filter(|(_, value)| value.len() >= 8)
        .collect();
    fragments.sort_unstable();
    fragments.dedup();
    for value in values {
        ranges.extend(
            text.match_indices(value.as_str())
                .map(|(start, found)| (start, start + found.len())),
        );
        for (fragment, candidate) in &fragments {
            if value.contains(candidate) {
                ranges.extend(
                    text.match_indices(fragment)
                        .map(|(start, found)| (start, start + found.len())),
                );
            }
        }
    }
    for pattern in PATTERNS.iter() {
        ranges.extend(pattern.captures_iter(text).filter_map(|captures| {
            // A boundary may be consumed to recognize a partial URL;
            // leave that separator and the following diagnostic intact.
            let found = captures
                .name("credential")
                .or_else(|| captures.get(0))
                .unwrap();
            if captures.name("credential").is_some() && has_numeric_port(found.as_str()) {
                // Without userinfo or another credential marker, this is
                // indistinguishable from a normal connection endpoint.
                // Known-value matches are still redacted independently.
                return None;
            }
            Some((found.start(), found.end()))
        }));
    }
    ranges
}

fn has_numeric_port(url: &str) -> bool {
    let Some((_, authority)) = url.split_once("://") else {
        return false;
    };
    let port = if let Some(bracketed) = authority.strip_prefix('[') {
        let Some((address, port)) = bracketed.split_once("]:") else {
            return false;
        };
        if address.parse::<std::net::Ipv6Addr>().is_err() {
            return false;
        }
        port
    } else {
        let Some((_, port)) = authority.split_once(':') else {
            return false;
        };
        port
    };
    !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) && port.parse::<u16>().is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn diagnostic_scrub_secret_keys_and_escaped_quotes() {
        for input in [
            "AWS_SECRET_ACCESS_KEY=opaque-aws-value",
            "SECRET_KEY=opaque-django-value",
            r#"{"password":"opaque\"private"}"#,
            r"password='opaque\'private'",
            r#"{"password":"opaque\"private"#,
        ] {
            let output = scrub(input, []);
            assert!(
                !output.contains("opaque") && !output.contains("private"),
                "{output}"
            );
        }
    }
    #[test]
    fn diagnostic_scrub_unfinished_quotes_before_another_line() {
        for quote in ['"', '\''] {
            for ending in ["\n", "\r\n"] {
                for suffix in ["", "\\"] {
                    let input = format!(
                        "password={quote}opaque\\{quote}private{suffix}{ending}permission denied /workspace"
                    );
                    let output = scrub(&input, []);
                    assert!(
                        !output.contains("opaque") && !output.contains("private"),
                        "{output}"
                    );
                    assert!(
                        output.ends_with("\npermission denied /workspace"),
                        "{output}"
                    );
                }
            }
        }
    }
    #[test]
    fn diagnostic_scrub_unknown_and_encoded_credentials() {
        for diagnostic in [
            "Authorization: Bearer synthetic-file-secret",
            "Basic c3ludGhldGljOnNlY3JldA==",
            "https://synthetic:password@example.test/path",
            "https://synthetic:partial",
            "api_key=synthetic-cut",
            "ENCRYPTION_KEY=synthetic-encryption",
            "SIGNING_KEY=synthetic-signing",
            "AUTH_KEY=synthetic-auth",
            "sk-synthetic-cut",
            "Authorization%3A%20Bearer%20synthetic-encoded",
            "{\"access_token\":\"synthetic-json\"}",
        ] {
            let output = scrub(diagnostic, []);
            assert!(!output.contains("synthetic"), "{output}");
        }
        let useful = "nono: parse error at line 3; permission denied /workspace";
        assert_eq!(scrub(useful, []), useful);
    }
    #[test]
    fn diagnostic_scrub_encoded_known_and_prefixed_fragment() {
        assert_eq!(
            scrub("opaque%41private", ["opaque%41private"]),
            "[REDACTED]"
        );
        assert_eq!(
            scrub(
                "token-sensitive-private",
                ["token", "token-sensitive-private-value"]
            ),
            "[REDACTED]"
        );
        assert_eq!(
            scrub("rose violet-extra", ["rose violet-extra"]),
            "[REDACTED]"
        );
        assert_eq!(
            scrub(
                "token%2Dsensitive%2Dprivate",
                ["token", "token-sensitive-private-value"]
            ),
            "[REDACTED]"
        );
        assert_eq!(
            scrub("nono: permission denied /a%20b", []),
            "nono: permission denied /a%20b"
        );
        assert_eq!(scrub("caf%C3%A9-private", ["café-private"]), "[REDACTED]");
        assert!(scrub("Authorization%3A%20Bearer%20%FF", []).contains("withheld"));
        let value = "synthetic-opaque-private";
        let encoded = base64::engine::general_purpose::STANDARD.encode(value);
        assert_eq!(scrub(&encoded, [value]), "[REDACTED]");
        assert_eq!(
            scrub("failed: opaque-private", [value]),
            "failed: [REDACTED]"
        );
        assert_eq!(scrub("synthetic%2Dopaque%2Dprivate", [value]), "[REDACTED]");
    }
    #[test]
    fn diagnostic_scrub_known_fragments_with_ellipsis() {
        for ellipsis in ["...", "…"] {
            assert_eq!(
                scrub(
                    &format!("failed: synthetic-private{ellipsis}"),
                    ["synthetic-private-value"]
                ),
                "failed: [REDACTED]"
            );
            assert_eq!(
                scrub(
                    &format!("failed: [abcdefgh{ellipsis}]"),
                    ["abcdefgh-private-value"]
                ),
                "failed: [[REDACTED]]"
            );
            let short = format!("abcdefg{ellipsis}");
            assert_eq!(scrub(&short, ["abcdefgh-private-value"]), short);
        }
        assert_eq!(
            scrub("synthetic-private%E2%80%A6", ["synthetic-private-value"]),
            "[REDACTED]"
        );
    }
    #[test]
    fn diagnostic_scrub_quoted_fragments_and_url_boundaries() {
        for ellipsis in ["...", "…"] {
            for input in [
                format!(r#"{{"error": "abcdefgh{ellipsis}"}}"#),
                format!(r#"{{"error":"abcdefgh{ellipsis}"}}"#),
                format!(r#"{{"error":"rose violet{ellipsis}"}}"#),
            ] {
                let output = scrub(&input, ["abcdefgh-private-value", "rose violet-private"]);
                let value: serde_json::Value = serde_json::from_str(&output).unwrap();
                assert_eq!(value["error"], "[REDACTED]");
            }
        }
        for ending in ["", "\npermission denied", "\r\npermission denied"] {
            let text = format!(r#"{{"error":"abcdefgh...{ending}"#);
            assert_eq!(
                scrub(&text, ["abcdefgh-private-value"]),
                format!(r#"{{"error":"[REDACTED]{ending}"#)
            );
        }
        for url in [
            "https://example.test?contact=alice@example.test",
            "https://example.test#contact=alice@example.test",
            "https://example.test?time=12:34",
            "https://example.test#time=12:34",
        ] {
            assert_eq!(scrub(url, []), url);
        }
        for url in [
            "https://synthetic:password@example.test?contact=alice@example.test",
            "https://synthetic:password@example.test#contact=alice@example.test",
        ] {
            assert_eq!(scrub(url, []), "[REDACTED]");
        }
    }
    #[test]
    fn diagnostic_scrub_partial_urls_before_boundaries() {
        for suffix in [
            "",
            "\n",
            "\r\n",
            " permission denied",
            "\npermission denied",
            "\r\npermission denied",
            "\" permission denied",
            "' permission denied",
            "\tpermission denied",
        ] {
            let text = format!("failed: https://alice:opaque-secret{suffix}");
            assert_eq!(scrub(&text, []), format!("failed: [REDACTED]{suffix}"));
        }
        assert_eq!(
            scrub(
                "https://alice:opaque-secret https://bob:other-private\n",
                []
            ),
            "[REDACTED] [REDACTED]\n"
        );
    }
    #[test]
    fn diagnostic_scrub_multiline_fragments_and_numeric_ports() {
        for ellipsis in ["...", "…"] {
            let input = format!("failed: 'rose\nviolet{ellipsis}'");
            assert_eq!(
                scrub(&input, ["rose\nviolet-extra"]),
                "failed: '[REDACTED]'"
            );
        }
        for endpoint in [
            "http://127.0.0.1:8080",
            "https://example.test:443",
            "http://localhost:3000",
            "http://[::1]:8080",
        ] {
            for suffix in ["", " connection refused", "\npermission denied"] {
                let input = format!("cannot connect to {endpoint}{suffix}");
                assert_eq!(scrub(&input, []), input);
            }
        }
        for url in [
            "https://alice:opaque:1234",
            "https://alice:opaque:1234 connection refused",
            "https://alice:opaque%3A1234\n",
        ] {
            assert!(!scrub(url, []).contains("opaque"));
        }
        assert_eq!(scrub("http://alice:12345@example.test", []), "[REDACTED]");
        assert!(!scrub("http://alice:12345 connection refused", ["12345"]).contains("12345"));
        assert_eq!(scrub("password=http://alice:12345", []), "[REDACTED]");
    }
    #[test]
    fn diagnostic_scrub_unknown_multiline_quoted_credentials() {
        for quote in ['"', '\''] {
            for newline in ["\n", "\r\n"] {
                let text = format!(
                    "password={quote}opaque{newline}private{quote}\npermission denied /workspace"
                );
                assert_eq!(scrub(&text, []), "[REDACTED]\npermission denied /workspace");
            }
        }
    }
    #[test]
    fn diagnostic_scrub_labelled_known_fragments() {
        for delimiter in [':', '='] {
            for ellipsis in ["...", "…"] {
                let text = format!("failed: error{delimiter}synthetic-private{ellipsis}");
                assert_eq!(
                    scrub(&text, ["synthetic-private-value"]),
                    "failed: [REDACTED]"
                );
                let short = format!("error{delimiter}abcdefg{ellipsis}");
                assert_eq!(scrub(&short, ["abcdefgh-private-value"]), short);
                assert_eq!(
                    scrub(
                        &format!("error{delimiter}abcdefgh{ellipsis}"),
                        ["abcdefgh-private-value"]
                    ),
                    "[REDACTED]"
                );
            }
        }
    }
    #[test]
    fn diagnostic_scrub_explicit_before_trimming_and_truncation() {
        assert_eq!(
            scrub(
                "nono: synthetic-private-value\n",
                ["synthetic-private-value\n"]
            )
            .trim(),
            "nono: [REDACTED]"
        );
        assert_eq!(
            scrub("nono: synthetic-private", ["synthetic-private-value"]),
            "nono: [REDACTED]"
        );
        assert_eq!(
            scrub("private-value", ["synthetic-private-value"]),
            "[REDACTED]"
        );
    }
}
