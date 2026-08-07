//! Public-surface disclosure inspection for the curated facade.
//!
//! The M5 preview surface and disclosure gate in
//! `docs/api/design-guidelines.md` forbids named implementation classes from
//! appearing anywhere in the facade's public signatures, and requires a
//! rustdoc leakage inspection over the complete public surface as review
//! evidence. This check is that inspection, and it is authoritative: reading
//! the sources never substitutes for it, because a disclosure can arrive
//! through a re-exported item this crate never mentions.
//!
//! Rustdoc renders an item from another crate as a link to that crate's
//! documentation, so a foreign type in a signature, bound, associated type, or
//! trait implementation is a link that leaves this crate. Prose that merely
//! names a crate is not a link, which is why the scan reads links rather than
//! words.
//!
//! The dependencies are documented rather than skipped. A crate that declares
//! no `html_root_url` has no address to link to under `--no-deps`, so rustdoc
//! renders its types as bare text and the disclosure becomes invisible;
//! documenting the dependency gives every foreign type a resolvable link. The
//! cost is one full documentation build.
//!
//! Blanket and synthetic implementation sections are excluded. They list what
//! other crates implement for every type rather than anything this surface
//! declares, so `impl<T> IntoEither for T` is noise here, not disclosure.
//!
//! An occurrence the facade review accepted stays listed in [`ACCEPTED`] with
//! the finding that tracks it. The list is exact in both directions: an
//! unlisted occurrence fails, and a listed occurrence that no longer exists
//! fails as well, so removing a disclosure also removes its entry.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;

/// The crate whose public surface the preview claims.
const FACADE: &str = "oxide-batch";

/// The rendered documentation directory for that crate.
const FACADE_DOCS: &str = "oxide_batch";

/// Link targets that are not a foreign crate.
///
/// `src` holds the rendered sources rather than an API surface, and the
/// standard library is the one ecosystem boundary the design guidelines
/// permit.
const NOT_FOREIGN: &[&str] = &[FACADE_DOCS, "src", "std", "core", "alloc", "proc_macro"];

/// The disclosure class a foreign dependency belongs to.
///
/// The prefixes cover the whole family a class ships under, so
/// `opentelemetry_sdk` and `tracing-subscriber` are classified without being
/// named individually. A dependency outside every prefix is still a
/// disclosure; it is only reported without a named class.
const CLASSES: &[(&str, &str)] = &[
    ("tokio", "async runtime"),
    ("sqlx", "database driver"),
    ("serde", "serializer"),
    ("opentelemetry", "telemetry SDK"),
    ("tracing", "telemetry SDK"),
];

/// One disclosure the facade review recorded as a finding rather than a pass.
struct Accepted {
    /// The rendered item the occurrence belongs to.
    item: &'static str,
    /// The dependency that item discloses.
    dependency: &'static str,
    /// Why the surface still carries it.
    reason: &'static str,
}

/// The disclosures the M5 facade review accepted as open findings.
///
/// Recorded by `docs/project/m5-facade-api-review-evidence.md`. Every entry is
/// a cross-crate seam the staged crate extraction widened from crate-private to
/// public. None is a documented application path, but each is callable, so
/// removing one is a pre-1.0 breaking change rather than a private cleanup.
const ACCEPTED: &[Accepted] = &[
    Accepted {
        item: "oxide_batch::ChunkComponentRevisions::manifest_value",
        dependency: "serde_json",
        reason: "core-to-plan canonical manifest seam, finding F1",
    },
    Accepted {
        item: "oxide_batch::DefinitionIdentity::from_flow_manifest",
        dependency: "serde_json",
        reason: "core-to-plan canonical manifest seam, finding F1",
    },
    Accepted {
        item: "oxide_batch::FlowTarget::manifest_value",
        dependency: "serde_json",
        reason: "core-to-plan canonical manifest seam, finding F1",
    },
    Accepted {
        item: "oxide_batch::StartControls::manifest_value",
        dependency: "serde_json",
        reason: "core-to-plan canonical manifest seam, finding F1",
    },
];

/// Runs the rendered-surface disclosure inspection.
///
/// Returns every violation as a human-readable line. An empty result means the
/// rendered surface discloses exactly the accepted occurrences and no other.
pub fn check() -> Result<Vec<String>, String> {
    build_documentation()?;
    let found = scan(&documentation_root()?)?;
    Ok(reconcile(&found))
}

/// Renders the complete public surface, including every optional adapter.
fn build_documentation() -> Result<(), String> {
    let status = Command::new("cargo")
        .args(["doc", "--package", FACADE, "--all-features"])
        .status()
        .map_err(|error| format!("could not run cargo doc: {error}"))?;

    if status.success() {
        Ok(())
    } else {
        Err(format!("cargo doc failed with {status}"))
    }
}

/// Locates the rendered documentation for the facade crate.
fn documentation_root() -> Result<PathBuf, String> {
    let output = Command::new("cargo")
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .output()
        .map_err(|error| format!("could not run cargo metadata: {error}"))?;

    if !output.status.success() {
        return Err(format!(
            "cargo metadata failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }

    let metadata: Value = serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("could not parse cargo metadata: {error}"))?;
    let target = metadata
        .get("target_directory")
        .and_then(Value::as_str)
        .ok_or_else(|| "cargo metadata returned no target directory".to_owned())?;

    Ok(Path::new(target).join("doc").join(FACADE_DOCS))
}

/// Returns every rendered item that links to a forbidden dependency.
fn scan(root: &Path) -> Result<BTreeSet<(String, String)>, String> {
    let mut found = BTreeSet::new();
    for page in pages(root)? {
        let rendered = fs::read_to_string(&page)
            .map_err(|error| format!("could not read {}: {error}", page.display()))?;
        found.extend(disclosures(&item_prefix(&page), &rendered));
    }
    Ok(found)
}

/// Returns every rendered page under `root`, including module directories.
fn pages(root: &Path) -> Result<Vec<PathBuf>, String> {
    let entries = fs::read_dir(root).map_err(|error| {
        format!(
            "could not read {}: {error}; render the documentation first",
            root.display()
        )
    })?;

    let mut found = Vec::new();
    for entry in entries {
        let path = entry
            .map_err(|error| format!("could not read {}: {error}", root.display()))?
            .path();
        if path.is_dir() {
            found.extend(pages(&path)?);
        } else if path
            .extension()
            .is_some_and(|extension| extension == "html")
        {
            found.push(path);
        }
    }
    Ok(found)
}

/// Renders the item path a page documents.
///
/// A rustdoc page is named `<kind>.<Item>.html`, so the middle component is
/// the item. Module and crate index pages document no item of their own.
fn item_prefix(page: &Path) -> String {
    let name = page
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let parts: Vec<&str> = name.trim_end_matches(".html").split('.').collect();

    match parts.as_slice() {
        [_kind, item] => format!("{FACADE_DOCS}::{item}"),
        _ => FACADE_DOCS.to_owned(),
    }
}

/// Returns the item and dependency of every foreign link on one page.
///
/// The owning item is the nearest anchor identifier before the link, which is
/// the method, associated item, or implementation block rustdoc rendered.
fn disclosures(prefix: &str, page: &str) -> BTreeSet<(String, String)> {
    const ANCHOR: &str = "id=\"";
    const LINK: &str = "href=\"";

    let rendered = declared(page);
    let mut found = BTreeSet::new();
    let mut anchor = String::new();
    let mut cursor = 0;

    while cursor < rendered.len() {
        let next_anchor = rendered[cursor..].find(ANCHOR).map(|at| cursor + at);
        let next_link = rendered[cursor..].find(LINK).map(|at| cursor + at);

        let (start, is_anchor) = match (next_anchor, next_link) {
            (Some(at), Some(link)) if at < link => (at + ANCHOR.len(), true),
            (_, Some(link)) => (link + LINK.len(), false),
            (Some(at), None) => (at + ANCHOR.len(), true),
            (None, None) => break,
        };

        if is_anchor {
            member(until(&rendered[start..], '"'))
                .unwrap_or_default()
                .clone_into(&mut anchor);
        } else if let Some(dependency) = foreign_crate(until(&rendered[start..], '"')) {
            found.insert((item(prefix, &anchor), dependency));
        }
        cursor = start;
    }

    found
}

/// Borrows the part of a page that documents what this surface declares.
///
/// Rustdoc appends the implementations other crates provide for every type
/// after a blanket or synthetic heading. Those describe the ecosystem rather
/// than the facade, so the scan stops there.
fn declared(page: &str) -> &str {
    const EXCLUDED: [&str; 2] = [
        "id=\"blanket-implementations\"",
        "id=\"synthetic-implementations\"",
    ];

    let end = EXCLUDED
        .iter()
        .filter_map(|heading| page.find(heading))
        .min()
        .unwrap_or(page.len());
    &page[..end]
}

/// Returns the foreign crate one rendered link resolves to, if it is foreign.
///
/// Rustdoc addresses a documented dependency relatively and a dependency that
/// declares an `html_root_url` absolutely, so both forms are read.
fn foreign_crate(href: &str) -> Option<String> {
    const ABSOLUTE: &str = "https://docs.rs/";

    let name = if let Some(rest) = href.strip_prefix(ABSOLUTE) {
        until(rest, '/')
    } else {
        let mut rest = href;
        while let Some(parent) = rest.strip_prefix("../") {
            rest = parent;
        }
        if rest == href {
            return None;
        }
        let name = until(rest, '/');
        if name.len() == rest.len() {
            return None;
        }
        name
    };

    let is_crate = !name.is_empty()
        && name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_');

    if is_crate && !NOT_FOREIGN.contains(&name) {
        Some(name.to_owned())
    } else {
        None
    }
}

/// Borrows an anchor that names a documented member of the page's item.
///
/// Rustdoc also anchors page furniture such as the copy-path control, which
/// owns nothing; ignoring it attributes a link to the page item instead of to
/// the button that happened to precede it.
fn member(anchor: &str) -> Option<&str> {
    const KINDS: [&str; 7] = [
        "method.",
        "tymethod.",
        "associatedtype.",
        "associatedconstant.",
        "structfield.",
        "variant.",
        "impl-",
    ];

    KINDS
        .iter()
        .any(|kind| anchor.starts_with(kind))
        .then_some(anchor)
}

/// Joins a page prefix and an anchor into one reported item path.
///
/// Rustdoc anchors carry their kind, so `method.decode` names the member and
/// `impl-Debug-for-JobName` names an implementation block.
fn item(prefix: &str, anchor: &str) -> String {
    match anchor.split_once('.') {
        Some((_kind, member)) if !member.is_empty() => format!("{prefix}::{member}"),
        _ if anchor.is_empty() => prefix.to_owned(),
        _ => format!("{prefix}#{anchor}"),
    }
}

/// Borrows the text before the next `terminator`.
fn until(rest: &str, terminator: char) -> &str {
    rest.split(terminator).next().unwrap_or_default()
}

/// Names the disclosure class a foreign dependency belongs to.
fn class_of(dependency: &str) -> &'static str {
    CLASSES
        .iter()
        .find(|(name, _)| dependency.starts_with(name))
        .map_or("implementation", |(_, class)| *class)
}

/// Renders the open findings this inspection carries.
pub fn accepted() -> Vec<String> {
    ACCEPTED
        .iter()
        .map(|entry| {
            format!(
                "{} discloses {} ({})",
                entry.item, entry.dependency, entry.reason
            )
        })
        .collect()
}

/// Reports every unaccepted disclosure and every accepted one that is gone.
fn reconcile(found: &BTreeSet<(String, String)>) -> Vec<String> {
    let accepted: BTreeSet<(String, String)> = ACCEPTED
        .iter()
        .map(|entry| (entry.item.to_owned(), entry.dependency.to_owned()))
        .collect();

    let mut violations = Vec::new();
    for (item, dependency) in found.difference(&accepted) {
        violations.push(format!(
            "{item} discloses the {} crate {dependency}",
            class_of(dependency)
        ));
    }
    for (item, dependency) in accepted.difference(found) {
        violations.push(format!(
            "{item} no longer discloses {dependency}; remove its accepted entry"
        ));
    }
    violations
}

#[cfg(test)]
mod tests {
    use super::{declared, disclosures, foreign_crate, item_prefix, reconcile};
    use std::collections::BTreeSet;
    use std::path::Path;

    /// Renders one method section the way rustdoc lays one out.
    fn method(anchor: &str, href: &str) -> String {
        format!(
            "<section id=\"{anchor}\" class=\"method\">\
             <h4 class=\"code-header\">pub fn <a href=\"#{anchor}\">member</a>() \
             -&gt; <a class=\"struct\" href=\"{href}\">Name</a></h4></section>"
        )
    }

    #[test]
    fn a_foreign_type_in_a_signature_is_attributed_to_its_member() {
        let page = method(
            "method.manifest_value",
            "https://docs.rs/serde_json/1/x.html",
        );

        assert_eq!(
            disclosures("oxide_batch::FlowTarget", &page),
            BTreeSet::from([(
                "oxide_batch::FlowTarget::manifest_value".to_owned(),
                "serde_json".to_owned()
            )])
        );
    }

    #[test]
    fn a_documented_dependency_is_read_from_its_relative_link() {
        let page = method("method.handle", "../tokio/runtime/struct.Handle.html");

        assert_eq!(
            disclosures("oxide_batch::JobLauncher", &page),
            BTreeSet::from([(
                "oxide_batch::JobLauncher::handle".to_owned(),
                "tokio".to_owned()
            )])
        );
    }

    #[test]
    fn page_furniture_never_owns_a_link() {
        let page = format!(
            "<a id=\"copy-path\"></a>{}",
            method("", "../tokio/runtime/struct.Handle.html")
        );

        assert_eq!(
            disclosures("oxide_batch::disclosure_probe", &page),
            BTreeSet::from([(
                "oxide_batch::disclosure_probe".to_owned(),
                "tokio".to_owned()
            )])
        );
    }

    #[test]
    fn blanket_implementations_are_not_this_surface() {
        let page = format!(
            "<h2 id=\"blanket-implementations\">{}",
            method("method.into_either", "https://docs.rs/either/1/x.html")
        );

        assert!(!declared(&page).contains("either"));
        assert!(disclosures("oxide_batch::JobName", &page).is_empty());
    }

    #[test]
    fn the_standard_library_and_this_crate_are_not_foreign() {
        assert_eq!(foreign_crate("struct.JobName.html"), None);
        assert_eq!(foreign_crate("../oxide_batch/struct.JobName.html"), None);
        assert_eq!(foreign_crate("../src/oxide_batch/lib.rs.html"), None);
        assert_eq!(
            foreign_crate("https://doc.rust-lang.org/1.97.1/core/option/enum.Option.html"),
            None
        );
        assert_eq!(foreign_crate("../static.files/main.js"), None);
        assert_eq!(
            foreign_crate("../sqlx/postgres/struct.PgPool.html"),
            Some("sqlx".to_owned())
        );
    }

    #[test]
    fn a_page_names_the_item_it_documents() {
        assert_eq!(
            item_prefix(Path::new("target/doc/oxide_batch/struct.JobName.html")),
            "oxide_batch::JobName"
        );
        assert_eq!(
            item_prefix(Path::new("target/doc/oxide_batch/index.html")),
            "oxide_batch"
        );
    }

    #[test]
    fn an_accepted_finding_that_is_gone_fails_as_loudly_as_a_new_one() {
        let repaired = BTreeSet::new();
        let violations = reconcile(&repaired);

        assert_eq!(violations.len(), super::ACCEPTED.len());
        assert!(
            violations
                .iter()
                .all(|line| line.contains("remove its accepted entry"))
        );
    }
}
