//! Best-effort credential scrubbing for untrusted launcher diagnostics.
//! Unknown opaque values without a credential marker cannot be recognized.
use base64::Engine;
use std::sync::LazyLock;

static PATTERNS: LazyLock<Vec<regex::Regex>> = LazyLock::new(|| {
    [
        r"(?im)(?:proxy-)?authorization\s*[=:]\s*[^\r\n]+",
        r#"(?i)\b(?:bearer|basic)\s+[^\s\"'<>]+"#,
        r#"(?i)\b[a-z][a-z0-9+.-]*://[^\s/?#\"'<>]*@[^\s\"'<>]*"#,
        r#"(?i)\b[a-z][a-z0-9+.-]*://[^\s/:?#\"'<>]+:[^\s/?#\"'<>]+$"#,
        r#"(?im)\b(?:[a-z0-9_]*[_-])?(?:api[_-]?key|access[_-]?token|refresh[_-]?token|token|secret(?:[_-](?:access[_-])?key)?|password|passwd|credential|client[_-]?secret|signature|private[_-]?key|(?:encryption|signing|auth)[_-]?key)\s*[\"']?\s*[:=]\s*(?:\"(?:\\[^\r\n]|[^\"\\\r\n])*(?:\"|\\?\r?$)|'(?:\\[^\r\n]|[^'\\\r\n])*(?:'|\\?\r?$)|[^\s&,;\"'<>]+)"#,
        r"\b(?:sk-|sk_|gh[pousr]_|github_pat_|xox[baprs]-|AKIA|ASIA|eyJ)[A-Za-z0-9_./+=%-]*",
    ].into_iter().map(|p| regex::Regex::new(p).expect("credential pattern")).collect()
});

// Extract complete quoted diagnostic values as well as whitespace tokens.
// Keep their original escaped bytes so matches map directly to the input.
static QUOTED_VALUES: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r#""(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'"#).expect("quoted values")
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
            QUOTED_VALUES
                .find_iter(text)
                .map(|quoted| &text[quoted.start() + 1..quoted.end() - 1]),
        )
        .map(|token| {
            // Compare the credential bytes, but redact the diagnostic's whole
            // fragment, including the marker that says it was truncated.
            let value = token
                .strip_suffix("...")
                .or_else(|| token.strip_suffix('…'))
                .unwrap_or(token);
            (token, value)
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
        ranges.extend(
            pattern
                .find_iter(text)
                .map(|found| (found.start(), found.end())),
        );
    }
    ranges
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
