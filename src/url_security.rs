use url::Url;

const MAX_HTTP_URL_BYTES: usize = 16 * 1024;

pub(crate) fn validate_http_endpoint(value: &str) -> Result<Url, &'static str> {
    if value.is_empty() || value.len() > MAX_HTTP_URL_BYTES || value.contains(['\0', '\n', '\r']) {
        return Err("must be non-empty, at most 16384 bytes, and contain no control characters");
    }
    let parsed = Url::parse(value).map_err(|_| "must be a valid absolute URL")?;
    if !matches!(parsed.scheme(), "http" | "https") || parsed.host().is_none() {
        return Err("must be an absolute http or https URL with a host");
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err("must not contain embedded user credentials");
    }
    if parsed.fragment().is_some() {
        return Err("must not contain a URL fragment");
    }
    Ok(parsed)
}

pub(crate) fn is_loopback_endpoint(url: &Url) -> bool {
    match url.host() {
        Some(url::Host::Ipv4(address)) => address.is_loopback(),
        Some(url::Host::Ipv6(address)) => address.is_loopback(),
        Some(url::Host::Domain(domain)) => {
            domain.eq_ignore_ascii_case("localhost")
                || domain.to_ascii_lowercase().ends_with(".localhost")
        }
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_embedded_credentials_and_fragments() {
        assert!(validate_http_endpoint("https://user:secret@example.test/mcp").is_err());
        assert!(validate_http_endpoint("https://example.test/mcp#secret").is_err());
    }

    #[test]
    #[cfg(any(feature = "temps-sandbox", feature = "codex"))]
    fn identifies_loopback_without_treating_lookalikes_as_local() {
        assert!(is_loopback_endpoint(
            &validate_http_endpoint("http://127.0.0.1:8787").unwrap()
        ));
        assert!(is_loopback_endpoint(
            &validate_http_endpoint("http://runtime.localhost:8787").unwrap()
        ));
        assert!(is_loopback_endpoint(
            &validate_http_endpoint("http://[::1]:8787").unwrap()
        ));
        assert!(!is_loopback_endpoint(
            &validate_http_endpoint("http://localhost.example.test").unwrap()
        ));
        assert!(!is_loopback_endpoint(
            &validate_http_endpoint("http://127.0.0.1.example.test").unwrap()
        ));
    }
}
