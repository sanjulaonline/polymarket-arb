use anyhow::{anyhow, Context, Result};
use reqwest::{ClientBuilder, Proxy};
use tokio::net::TcpStream;
use tokio_socks::tcp::{Socks4Stream, Socks5Stream};
use tokio_tungstenite::{
    client_async_tls_with_config, connect_async,
    tungstenite::{
        client::IntoClientRequest,
        handshake::client::Response,
    },
    MaybeTlsStream, WebSocketStream,
};
use url::Url;

fn read_env(name: &str) -> String {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_default()
}

fn parse_proxy_url(raw: &str) -> Result<Url> {
    Url::parse(raw).with_context(|| format!("invalid proxy URL: {raw}"))
}

pub fn get_proxy_url_for(target_url: &str) -> Option<Url> {
    let u = target_url.trim().to_ascii_lowercase();
    let is_https = u.starts_with("https://") || u.starts_with("wss://");
    let is_http = u.starts_with("http://") || u.starts_with("ws://");

    let all = read_env("ALL_PROXY");
    let all_lower = if all.is_empty() {
        read_env("all_proxy")
    } else {
        all
    };
    let all = all_lower;

    let https = read_env("HTTPS_PROXY");
    let https_lower = if https.is_empty() {
        read_env("https_proxy")
    } else {
        https
    };
    let https = https_lower;

    let http = read_env("HTTP_PROXY");
    let http_lower = if http.is_empty() {
        read_env("http_proxy")
    } else {
        http
    };
    let http = http_lower;

    let chosen = if is_https {
        if !https.is_empty() {
            https
        } else {
            all
        }
    } else if is_http {
        if !http.is_empty() {
            http
        } else {
            all
        }
    } else if !all.is_empty() {
        all
    } else if !https.is_empty() {
        https
    } else {
        http
    };

    if chosen.is_empty() {
        None
    } else {
        parse_proxy_url(&chosen).ok()
    }
}

fn is_socks_scheme(scheme: &str) -> bool {
    matches!(
        scheme,
        "socks" | "socks5" | "socks5h" | "socks4" | "socks4a"
    )
}

fn reqwest_proxy_http(raw: &str) -> Result<Proxy> {
    let parsed = parse_proxy_url(raw)?;
    let scheme = parsed.scheme().to_ascii_lowercase();
    if is_socks_scheme(&scheme) {
        Proxy::all(raw).with_context(|| format!("invalid proxy for HTTP traffic: {raw}"))
    } else {
        Proxy::http(raw).with_context(|| format!("invalid proxy for HTTP traffic: {raw}"))
    }
}

fn reqwest_proxy_https(raw: &str) -> Result<Proxy> {
    let parsed = parse_proxy_url(raw)?;
    let scheme = parsed.scheme().to_ascii_lowercase();
    if is_socks_scheme(&scheme) {
        Proxy::all(raw).with_context(|| format!("invalid proxy for HTTPS traffic: {raw}"))
    } else {
        Proxy::https(raw).with_context(|| format!("invalid proxy for HTTPS traffic: {raw}"))
    }
}

pub fn apply_reqwest_proxy_from_env(mut builder: ClientBuilder) -> Result<ClientBuilder> {
    let all = read_env("ALL_PROXY");
    let all = if all.is_empty() {
        read_env("all_proxy")
    } else {
        all
    };

    let https = read_env("HTTPS_PROXY");
    let https = if https.is_empty() {
        read_env("https_proxy")
    } else {
        https
    };

    let http = read_env("HTTP_PROXY");
    let http = if http.is_empty() {
        read_env("http_proxy")
    } else {
        http
    };

    if !all.is_empty() {
        let proxy = Proxy::all(&all).with_context(|| format!("invalid ALL_PROXY: {all}"))?;
        builder = builder.proxy(proxy);
    }
    if !http.is_empty() {
        let proxy = reqwest_proxy_http(&http)?;
        builder = builder.proxy(proxy);
    }
    if !https.is_empty() {
        let proxy = reqwest_proxy_https(&https)?;
        builder = builder.proxy(proxy);
    }

    Ok(builder)
}

pub async fn connect_ws_with_proxy<R>(request: R) -> Result<(WebSocketStream<MaybeTlsStream<TcpStream>>, Response)>
where
    R: IntoClientRequest + Unpin,
{
    let request = request.into_client_request()?;
    let target_url = request.uri().to_string();

    let Some(proxy) = get_proxy_url_for(&target_url) else {
        return connect_async(request).await.map_err(Into::into);
    };

    let scheme = request.uri().scheme_str().unwrap_or("wss");
    let target_host = request
        .uri()
        .host()
        .context("websocket target host is missing")?
        .to_string();
    let target_port = request.uri().port_u16().unwrap_or(match scheme {
        "ws" | "http" => 80,
        _ => 443,
    });

    let proxy_host = proxy
        .host_str()
        .context("proxy host is missing")?
        .to_string();
    let proxy_port = proxy.port_or_known_default().unwrap_or(8080);
    let proxy_scheme = proxy.scheme().to_ascii_lowercase();

    let stream = match proxy_scheme.as_str() {
        "http" => {
            let mut stream = TcpStream::connect((proxy_host.as_str(), proxy_port)).await?;
            async_http_proxy::http_connect_tokio(&mut stream, target_host.as_str(), target_port)
                .await
                .with_context(|| {
                    format!(
                        "HTTP CONNECT failed via proxy {}:{} to {}:{}",
                        proxy_host, proxy_port, target_host, target_port
                    )
                })?;
            stream
        }
        "socks" | "socks5" | "socks5h" => {
            Socks5Stream::connect(
                (proxy_host.as_str(), proxy_port),
                (target_host.as_str(), target_port),
            )
            .await
            .with_context(|| {
                format!(
                    "SOCKS5 connect failed via proxy {}:{} to {}:{}",
                    proxy_host, proxy_port, target_host, target_port
                )
            })?
            .into_inner()
        }
        "socks4" | "socks4a" => {
            Socks4Stream::connect(
                (proxy_host.as_str(), proxy_port),
                (target_host.as_str(), target_port),
            )
            .await
            .with_context(|| {
                format!(
                    "SOCKS4 connect failed via proxy {}:{} to {}:{}",
                    proxy_host, proxy_port, target_host, target_port
                )
            })?
            .into_inner()
        }
        "https" => {
            return Err(anyhow!(
                "HTTPS proxy URLs are not supported for websocket tunneling in this build: {}",
                proxy
            ));
        }
        other => {
            return Err(anyhow!("unsupported proxy scheme for websocket: {other}"));
        }
    };

    let (ws, response) = client_async_tls_with_config(request, stream, None, None).await?;
    Ok((ws, response))
}