//! `verify-full` TLS as the supported production transport.
//!
//! The M5 preview supports one transport for a production `PostgreSQL`
//! deployment: TLS with the server's certificate chain and its host name both
//! validated. This target is that report. It is not a report about a flag being
//! readable — it opens the real repository, through the real configuration
//! type, against a server that really speaks TLS, and it does so four times.
//!
//! Once it must succeed. A private authority signs a certificate for the name
//! the report connects to, the server presents it, and the supported
//! configuration opens, migrates, and reads. That the session was actually
//! encrypted is not taken from the client's own account of itself: the server
//! is asked, through a separate administrative connection, what transport the
//! adapter's live backends are using.
//!
//! Three times it must fail, and for three different reasons, each isolated so
//! that one mechanism is under test at a time:
//!
//! - an authority that signed nothing the server presents, with the name
//!   correct, which certificate validation must refuse;
//! - the correct authority, reaching the same server under a name its
//!   certificate does not carry, which host-name validation must refuse;
//! - the correct authority and a reachable server that offers no TLS at all,
//!   which the supported mode must refuse rather than downgrade to plaintext.
//!
//! The third is the one that makes the other two mean anything. A client that
//! quietly fell back to an unencrypted session whenever TLS was unavailable
//! would still refuse a bad certificate, and would still be unsafe.
//!
//! Two further things are required of the failures. The adapter deliberately
//! reports every connection failure as one redacted, unclassified error, so the
//! report cannot learn from it *why* a connection was refused. It therefore
//! corroborates each refusal at the transport layer and requires the reason to
//! be the one the attempt was built to provoke — an untrusted authority must
//! fail on the issuer, not on the name — and it requires the transport probe
//! and the production path to agree on every attempt, so the two cannot drift
//! apart unnoticed. And after each refusal the server is asked again whether
//! any unencrypted session was opened on the adapter's behalf. None may be, at
//! any point in the report.
//!
//! Nothing here adds a "production mode" switch to obtain this behaviour. The
//! report builds [`PostgresConfig`] the way the support contract says an
//! operator does and takes what that gives, which is `verify-full` by default;
//! and it requires the configuration surface to keep refusing a TLS option
//! smuggled through the connection string, which is the only other way a
//! deployment could express a weaker transport.

#![cfg(feature = "postgres")]

mod security;

use std::error::Error;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use oxide_batch::{
    PostgresConfig, PostgresConfigError, PostgresJobRepository, PostgresMigrator, RepositoryError,
};
use serde_json::{Value, json};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};
use sqlx::{Connection, PgConnection, Row};

use security::{
    Failure, FixedClock, admin_url, major_version, plaintext_url, read_ca, recreate_database,
    retain_observation, server_version, supported_config, tls_ca, tls_host, tls_mismatch_host,
    tls_untrusted_ca, with_database, with_host,
};

/// The database this report builds and reports on.
const DATABASE: &str = "oxide_batch_m5_security_tls";

/// The application name the adapter gives every connection it opens.
///
/// The server-side readings below are filtered by it, so what they describe is
/// the adapter's own sessions rather than the report's fixture connections.
const APPLICATION_NAME: &str = "oxide-batch";

/// What the supported configuration must do with one set of TLS material.
#[derive(Clone, Copy, Eq, PartialEq)]
enum Expectation {
    /// The repository must open.
    Connects,
    /// The repository must refuse, for the named transport reason.
    Refused(FailureClass),
}

/// The transport reason one refusal must have.
///
/// The class is required rather than merely recorded. An attempt built around
/// an untrusted authority that failed because the host name did not match would
/// be a green result that proved nothing about certificate validation.
#[derive(Clone, Copy, Eq, PartialEq)]
enum FailureClass {
    /// The presented chain did not lead to a trusted authority.
    UntrustedAuthority,
    /// The chain was trusted and did not cover the name connected to.
    HostnameMismatch,
    /// The server offered no TLS, and the supported mode did not downgrade.
    TlsNotOffered,
}

impl FailureClass {
    /// Returns the stable name the retained evidence uses.
    const fn as_str(self) -> &'static str {
        match self {
            Self::UntrustedAuthority => "untrusted-authority",
            Self::HostnameMismatch => "hostname-mismatch",
            Self::TlsNotOffered => "tls-not-offered",
        }
    }

    /// Classifies one transport failure without retaining its text.
    ///
    /// The text of a connection failure is not recorded anywhere: it can carry
    /// the host, the port, and the name the certificate was issued for. Only
    /// the class it maps to leaves this function.
    fn classify(error: &sqlx::Error) -> Option<Self> {
        let text = format!("{error}");
        if text.contains("UnknownIssuer") {
            return Some(Self::UntrustedAuthority);
        }
        if text.contains("certificate not valid for name") {
            return Some(Self::HostnameMismatch);
        }
        if text.contains("does not support TLS") {
            return Some(Self::TlsNotOffered);
        }
        None
    }
}

/// One connection the report makes and what the contract requires of it.
struct Attempt {
    /// The identifier the retained evidence uses.
    id: &'static str,
    /// What the attempt is built to exercise.
    intent: &'static str,
    /// The URL, which differs from the others only in the way under test.
    url: String,
    /// Where to ask what sessions the adapter has on the server it targeted.
    ///
    /// This is the attempt's own server. Asking the TLS server whether a
    /// connection to a different one downgraded would answer a question nobody
    /// asked, and would answer it reassuringly.
    sessions_url: String,
    /// The authority the supported configuration is given.
    authority: Authority,
    /// What the support contract requires of it.
    expectation: Expectation,
}

/// Which certificate authority one attempt trusts.
#[derive(Clone, Copy, Eq, PartialEq)]
enum Authority {
    /// The authority that signed the server's certificate.
    Signing,
    /// An authority that signed nothing the server presents.
    Unrelated,
}

impl Authority {
    /// Returns the stable name the retained evidence uses.
    const fn as_str(self) -> &'static str {
        match self {
            Self::Signing => "signing",
            Self::Unrelated => "unrelated",
        }
    }
}

#[test]
fn verify_full_tls_is_required_in_the_supported_mode() -> Result<(), Box<dyn Error>> {
    let Some(admin) = admin_url() else {
        eprintln!("skipped: OXIDEBATCH_POSTGRES_ADMIN_TEST_URL is not set");
        return Ok(());
    };
    let Some(host) = tls_host() else {
        eprintln!("skipped: OXIDEBATCH_SECURITY_TLS_HOST is not set");
        return Ok(());
    };
    let Some(mismatch) = tls_mismatch_host() else {
        eprintln!("skipped: OXIDEBATCH_SECURITY_TLS_MISMATCH_HOST is not set");
        return Ok(());
    };
    let Some(signing) = tls_ca() else {
        eprintln!("skipped: OXIDEBATCH_SECURITY_TLS_CA is not set");
        return Ok(());
    };
    let Some(unrelated) = tls_untrusted_ca() else {
        eprintln!("skipped: OXIDEBATCH_SECURITY_TLS_UNTRUSTED_CA is not set");
        return Ok(());
    };
    let Some(plaintext) = plaintext_url() else {
        eprintln!("skipped: OXIDEBATCH_SECURITY_PLAINTEXT_TEST_URL is not set");
        return Ok(());
    };

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run_report(Fixture {
            admin,
            host,
            mismatch,
            signing,
            unrelated,
            plaintext,
        }))
}

/// Everything the fixture supplies, resolved.
struct Fixture {
    /// A connection able to create the database the report opens.
    admin: String,
    /// The name the server's certificate is issued for.
    host: String,
    /// A name that reaches the same server and the certificate does not carry.
    mismatch: String,
    /// The authority that signed the server's certificate.
    signing: PathBuf,
    /// An authority that signed nothing the server presents.
    unrelated: PathBuf,
    /// A reachable endpoint that offers no TLS.
    plaintext: String,
}

/// Makes every attempt and requires the supported transport of each.
async fn run_report(fixture: Fixture) -> Result<(), Box<dyn Error>> {
    let server = server_version(&fixture.admin).await?;
    let database = with_database(&fixture.admin, DATABASE)?;
    recreate_database(&fixture.admin, DATABASE).await?;

    // The database the report opens is migrated over the supported transport
    // too. A campaign that migrated in plaintext and then only read over TLS
    // would leave the schema-owning path unreported.
    let migrating = with_host(&database, &fixture.host)?;
    let signing_ca = read_ca(&fixture.signing)?;
    PostgresMigrator::migrate(&supported_config(
        migrating.clone(),
        Some(signing_ca.clone()),
    )?)
    .await?;

    let attempts = vec![
        Attempt {
            id: "trusted-authority-and-name",
            intent: "the supported production configuration against a server whose certificate \
                     the given authority signed for the name connected to",
            url: migrating.clone(),
            sessions_url: database.clone(),
            authority: Authority::Signing,
            expectation: Expectation::Connects,
        },
        Attempt {
            id: "untrusted-authority",
            intent: "the same server and the same name, trusting an authority that signed \
                     nothing it presents",
            url: migrating.clone(),
            sessions_url: database.clone(),
            authority: Authority::Unrelated,
            expectation: Expectation::Refused(FailureClass::UntrustedAuthority),
        },
        Attempt {
            id: "hostname-mismatch",
            intent: "the same server and the signing authority, reached under a name its \
                     certificate does not carry",
            url: with_host(&database, &fixture.mismatch)?,
            sessions_url: database.clone(),
            authority: Authority::Signing,
            expectation: Expectation::Refused(FailureClass::HostnameMismatch),
        },
        Attempt {
            id: "server-without-tls",
            intent: "a reachable server that offers no TLS, which the supported mode must \
                     refuse rather than continue without encryption",
            url: fixture.plaintext.clone(),
            // The session reading for this attempt is taken on the plaintext
            // server, because that is where a downgraded connection would be.
            sessions_url: fixture.plaintext.clone(),
            authority: Authority::Signing,
            expectation: Expectation::Refused(FailureClass::TlsNotOffered),
        },
    ];

    let mut results = Vec::new();
    for attempt in &attempts {
        results.push(run_attempt(attempt, &fixture, &signing_ca).await?);
    }

    // The only other way a deployment could ask for a weaker transport is to
    // put it in the connection string, and the configuration surface refuses
    // that rather than honouring it.
    let smuggled = smuggled_tls_option(&migrating);

    // Nothing the report opened may outlive it, encrypted or not. A pool that
    // survived would keep a session the next reading could mistake for its own.
    let residual = adapter_sessions(&database).await?;
    assert_eq!(
        residual.plaintext, 0,
        "the adapter left an unencrypted session behind after the report finished",
    );

    retain_observation(
        "verify-full-tls",
        &json!({
            "report": "verify-full TLS in the supported mode",
            "scenario": "verify_full_tls_is_required_in_the_supported_mode",
            "fixture": "postgres-security-tls",
            "server_version": server,
            "postgres_major_version": major_version(&server),
            "tls_mode": "verify-full",
            "tls_mode_source": "the PostgresConfig default, with no production-mode switch",
            "certificate_validation_result": class_result(&results, FailureClass::UntrustedAuthority),
            "hostname_validation_result": class_result(&results, FailureClass::HostnameMismatch),
            "plaintext_fallback_result": class_result(&results, FailureClass::TlsNotOffered),
            "residual_sessions_encrypted": residual.encrypted,
            "residual_sessions_plaintext": residual.plaintext,
            "tls_option_in_connection_string": smuggled,
            "attempts": results,
            "violations": Vec::<String>::new(),
            "passed": true,
        }),
    )?;

    Ok(())
}

/// Reports what one attempt did, and requires the contract of it.
#[allow(
    clippy::too_many_lines,
    reason = "opening the supported configuration, corroborating the refusal at the transport \
              layer, and asking the server what it accepted form one attempt that is only \
              meaningful in order"
)]
async fn run_attempt(
    attempt: &Attempt,
    fixture: &Fixture,
    signing: &oxide_batch::CaCertificate,
) -> Result<Value, Box<dyn Error>> {
    let database = attempt.sessions_url.as_str();
    let authority = match attempt.authority {
        Authority::Signing => signing.clone(),
        Authority::Unrelated => read_ca(&fixture.unrelated)?,
    };
    let authority_path = match attempt.authority {
        Authority::Signing => fixture.signing.as_path(),
        Authority::Unrelated => fixture.unrelated.as_path(),
    };

    // The production path: the supported configuration, the real repository.
    let config = supported_config(attempt.url.clone(), Some(authority.clone()))?;
    let opened = PostgresJobRepository::connect(config, Arc::new(FixedClock(UNIX_EPOCH))).await;

    // The transport probe: the same URL and the same authority, connected
    // directly, so the refusal can be classified. The adapter reports every
    // connection failure as one redacted error by design, so this is the only
    // way to require a refusal to have the reason it was built to have.
    let probed = probe_transport(&attempt.url, authority_path).await;

    assert_eq!(
        opened.is_ok(),
        probed.is_ok(),
        "{}: the supported configuration and the transport probe disagreed about whether this \
         connection is possible, so the probe no longer describes what the adapter does",
        attempt.id,
    );

    let observed = match (&attempt.expectation, opened) {
        (Expectation::Connects, Ok(repository)) => {
            let sessions = adapter_sessions(database).await?;
            assert!(
                sessions.encrypted > 0,
                "{}: the repository opened and the server reports no encrypted session for it",
                attempt.id,
            );
            assert_eq!(
                sessions.plaintext, 0,
                "{}: the repository opened an unencrypted session",
                attempt.id,
            );
            let transport = sessions.transport.clone();
            repository.close().await?;
            json!({
                "result": "connected",
                "encrypted_sessions": sessions.encrypted,
                "plaintext_sessions": sessions.plaintext,
                "transport": transport,
            })
        }
        (Expectation::Connects, Err(error)) => {
            return Err(Box::new(Failure(format!(
                "{}: the supported configuration must connect and was refused with {error}",
                attempt.id,
            ))));
        }
        (Expectation::Refused(class), Ok(repository)) => {
            repository.close().await?;
            return Err(Box::new(Failure(format!(
                "{}: the supported configuration must refuse this connection as {} and it \
                 connected",
                attempt.id,
                class.as_str(),
            ))));
        }
        (Expectation::Refused(class), Err(error)) => {
            assert_eq!(
                error,
                RepositoryError::Unavailable,
                "{}: a refused connection must stay the adapter's one redacted failure",
                attempt.id,
            );

            let probe_error = probed.err().ok_or_else(|| {
                Failure(format!(
                    "{}: the transport probe recorded no failure",
                    attempt.id
                ))
            })?;
            let observed_class = FailureClass::classify(&probe_error).ok_or_else(|| {
                Failure(format!(
                    "{}: the refusal could not be classified as a certificate, host name, or \
                     absent-TLS failure, so the report cannot say the transport refused it for \
                     the reason under test",
                    attempt.id,
                ))
            })?;
            assert!(
                observed_class == *class,
                "{}: this attempt is built to be refused as {} and the transport refused it as {}",
                attempt.id,
                class.as_str(),
                observed_class.as_str(),
            );

            // Nothing may have been opened in place of the refused connection.
            let sessions = adapter_sessions(database).await?;
            assert_eq!(
                sessions.plaintext, 0,
                "{}: a refused TLS connection was followed by an unencrypted session",
                attempt.id,
            );
            json!({
                "result": "refused",
                "failure_class": observed_class.as_str(),
                "repository_error": "unavailable",
                "plaintext_sessions": sessions.plaintext,
            })
        }
    };

    Ok(json!({
        "id": attempt.id,
        "intent": attempt.intent,
        "authority": attempt.authority.as_str(),
        "expected": match attempt.expectation {
            Expectation::Connects => "connects".to_owned(),
            Expectation::Refused(class) => format!("refused: {}", class.as_str()),
        },
        "observed": observed,
    }))
}

/// Connects at the transport layer so a refusal can be classified.
///
/// This is the one place the report reaches past the facade, and it exists
/// because the facade is deliberately silent about why a connection failed.
/// Its outcome is required to agree with the production path on every attempt,
/// so it cannot drift into describing a different connection.
async fn probe_transport(url: &str, authority: &Path) -> Result<(), sqlx::Error> {
    let options = PgConnectOptions::from_str(url)?
        .application_name(APPLICATION_NAME)
        .ssl_mode(PgSslMode::VerifyFull)
        .ssl_root_cert(authority);
    let connection = PgConnection::connect_with(&options).await?;
    connection.close().await
}

/// What the server says about the transport of the adapter's own sessions.
#[derive(Clone, Debug, Default)]
struct Sessions {
    /// Backends the adapter opened that are using TLS.
    encrypted: i64,
    /// Backends the adapter opened that are not.
    plaintext: i64,
    /// The TLS versions those sessions negotiated.
    transport: Vec<String>,
}

/// Asks the server what transport the adapter's live sessions are using.
///
/// The reading is taken through a separate administrative connection rather
/// than through the session being described. A client that had downgraded
/// would report whatever it believed about itself; the server reports what it
/// actually accepted.
async fn adapter_sessions(database_url: &str) -> Result<Sessions, Box<dyn Error>> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await?;
    let rows = sqlx::query(
        "SELECT encryption.ssl AS encrypted, coalesce(encryption.version, '') AS version \
         FROM pg_stat_activity activity \
         JOIN pg_stat_ssl encryption ON encryption.pid = activity.pid \
         WHERE activity.application_name = $1 \
           AND activity.datname = current_database() \
           AND activity.pid <> pg_backend_pid()",
    )
    .bind(APPLICATION_NAME)
    .fetch_all(&pool)
    .await?;
    pool.close().await;

    let mut sessions = Sessions::default();
    for row in &rows {
        if row.try_get::<bool, _>("encrypted")? {
            sessions.encrypted += 1;
            let version: String = row.try_get("version")?;
            if !version.is_empty() && !sessions.transport.contains(&version) {
                sessions.transport.push(version);
            }
        } else {
            sessions.plaintext += 1;
        }
    }
    sessions.transport.sort();
    Ok(sessions)
}

/// Requires the configuration surface to refuse a smuggled TLS option.
///
/// `verify-full` being the default is only half of the contract. The other half
/// is that a connection string cannot quietly replace it, which is how a
/// deployment would otherwise end up in a weaker mode without changing any code
/// that review would see.
fn smuggled_tls_option(url: &str) -> Value {
    let mut refused = Vec::new();
    for option in ["sslmode=disable", "sslmode=prefer", "sslrootcert=/dev/null"] {
        let attempt = PostgresConfig::new(format!("{url}?{option}"));
        assert_eq!(
            attempt.err(),
            Some(PostgresConfigError::TlsOptionInConnectionString),
            "a connection string carrying {option} must be refused rather than allowed to \
             replace the supported transport",
        );
        refused.push(option.to_owned());
    }
    json!({ "refused_options": refused })
}

/// Reports what the attempt built around one failure class observed.
fn class_result(results: &[Value], class: FailureClass) -> Value {
    let expected = format!("refused: {}", class.as_str());
    results
        .iter()
        .find(|result| result.get("expected").and_then(Value::as_str) == Some(expected.as_str()))
        .and_then(|result| result.get("observed"))
        .and_then(|observed| observed.get("result"))
        .cloned()
        .unwrap_or(Value::Null)
}
