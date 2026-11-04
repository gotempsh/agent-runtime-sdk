//! Best-effort credential scrubbing for untrusted launcher diagnostics.
//! Unknown opaque values without a credential marker cannot be recognized.
use base64::Engine;
use std::sync::LazyLock;

static PATTERNS: LazyLock<Vec<regex::Regex>> = LazyLock::new(|| {
    [
        r"(?im)(?:proxy-)?authorization\s*[=:]\s*[^\r\n]+",
        r#"(?i)\b(?:bearer|basic)\s+[^\s\"'<>]+"#,
        r#"(?i)\b[a-z][a-z0-9+.-]*://[^\s/\"'<>]*@[^\s\"'<>]*"#,
        r#"(?i)\b[a-z][a-z0-9+.-]*://[^\s/:\"'<>]+:[^\s/\"'<>]+$"#,
        r#"(?i)\b(?:[a-z0-9_]*[_-])?(?:api[_-]?key|access[_-]?token|refresh[_-]?token|token|secret|password|passwd|credential|client[_-]?secret|signature|private[_-]?key)\s*[\"']?\s*[:=]\s*(?:\"[^\"\r\n]*(?:\"|$)|'[^'\r\n]*(?:'|$)|[^\s&,;\"'<>]+)"#,
        r"\b(?:sk-|sk_|gh[pousr]_|github_pat_|xox[baprs]-|AKIA|ASIA|eyJ)[A-Za-z0-9_./+=%-]*",
    ].into_iter().map(|p| regex::Regex::new(p).expect("credential pattern")).collect()
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
    // Exact values must be removed before normalization changes their bytes.
    let mut raw = text.to_owned();
    for value in &values {
        raw = raw.replace(value, "[REDACTED]");
    }
    // Decode percent escapes before matching, including encoded header delimiters.
    let mut bytes = Vec::with_capacity(text.len());
    let input = raw.as_bytes();
    let mut i = 0;
    while i < input.len() {
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
    let mut text = String::from_utf8_lossy(&bytes).into_owned();
    for value in values {
        text = text.replace(&value, "[REDACTED]");
        // Producers may have cut a known value before we see the diagnostic.
        // Also match bounded token fragments after a runtime-added error prefix.
        let fragments: Vec<String> = text
            .split_whitespace()
            .map(|token| token.trim_matches(|c: char| "\"'(),;[]".contains(c)))
            .filter(|token| token.len() >= 8 && value.contains(token))
            .map(str::to_owned)
            .collect();
        for fragment in fragments {
            text = text.replace(&fragment, "[REDACTED]");
        }
    }
    for pattern in PATTERNS.iter() {
        text = pattern.replace_all(&text, "[REDACTED]").into_owned();
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn diagnostic_scrub_unknown_and_encoded_credentials() {
        for diagnostic in [
            "Authorization: Bearer synthetic-file-secret",
            "Basic c3ludGhldGljOnNlY3JldA==",
            "https://synthetic:password@example.test/path",
            "https://synthetic:partial",
            "api_key=synthetic-cut",
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
