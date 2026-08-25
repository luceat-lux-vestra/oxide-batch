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

/// One disclosure an accepted decision record permits.
struct Accepted {
    /// The rendered item the occurrence belongs to.
    item: &'static str,
    /// The dependency that item discloses.
    dependency: &'static str,
    /// The ADR that approved the exception, and its removal plan.
    reason: &'static str,
}

/// The disclosures the facade review accepted as open findings.
///
/// The list is empty, and adding to it is not how a disclosure gets approved.
/// The API design guidelines require an accepted ADR before a forbidden class
/// appears in a public signature; an entry here only records where an approved
/// exception already applies, and carries the ADR that approved it. An entry
/// without one is the exception approving itself.
const ACCEPTED: &[Accepted] = &[
    Accepted {
        item: "oxide_batch::ItemReader#impl-ItemReader%3CI%3E-for-JsonArrayReader%3CSrc%3E",
        dependency: "serde_json",
        reason: "ADR-0012: JSON item representation is serde_json::Value directly",
    },
    Accepted {
        item: "oxide_batch::ItemReader#impl-ItemReader%3CI%3E-for-JsonLinesReader%3CSrc%3E",
        dependency: "serde_json",
        reason: "ADR-0012: JSON item representation is serde_json::Value directly",
    },
    Accepted {
        item: "oxide_batch::ItemWriter#impl-ItemWriter%3CI%3E-for-JsonArrayWriter",
        dependency: "serde_json",
        reason: "ADR-0012: JSON item representation is serde_json::Value directly",
    },
    Accepted {
        item: "oxide_batch::ItemWriter#impl-ItemWriter%3CI%3E-for-JsonLinesWriter",
        dependency: "serde_json",
        reason: "ADR-0012: JSON item representation is serde_json::Value directly",
    },
    Accepted {
        item: "oxide_batch::JsonArrayReader#impl-ItemReader%3CI%3E-for-JsonArrayReader%3CSrc%3E",
        dependency: "serde_json",
        reason: "ADR-0012: JSON item representation is serde_json::Value directly",
    },
    Accepted {
        item: "oxide_batch::JsonArrayWriter#impl-ItemWriter%3CI%3E-for-JsonArrayWriter",
        dependency: "serde_json",
        reason: "ADR-0012: JSON item representation is serde_json::Value directly",
    },
    Accepted {
        item: "oxide_batch::JsonLinesReader#impl-ItemReader%3CI%3E-for-JsonLinesReader%3CSrc%3E",
        dependency: "serde_json",
        reason: "ADR-0012: JSON item representation is serde_json::Value directly",
    },
    Accepted {
        item: "oxide_batch::JsonLinesWriter#impl-ItemWriter%3CI%3E-for-JsonLinesWriter",
        dependency: "serde_json",
        reason: "ADR-0012: JSON item representation is serde_json::Value directly",
    },
    Accepted {
        item: "oxide_batch::json_array_file_reader",
        dependency: "serde_json",
        reason: "ADR-0012: JSON item representation is serde_json::Value directly",
    },
    Accepted {
        item: "oxide_batch::json_array_reader",
        dependency: "serde_json",
        reason: "ADR-0012: JSON item representation is serde_json::Value directly",
    },
    Accepted {
        item: "oxide_batch::jsonl_file_reader",
        dependency: "serde_json",
        reason: "ADR-0012: JSON item representation is serde_json::Value directly",
    },
    Accepted {
        item: "oxide_batch::jsonl_reader",
        dependency: "serde_json",
        reason: "ADR-0012: JSON item representation is serde_json::Value directly",
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
        found.extend(disclosures(
            &item_prefix(&page),
            depth_of(&page, root),
            &rendered,
        ));
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

/// Returns the item and dependency of every foreign link one page declares.
///
/// Only rendered declarations are read — the item signature block and the
/// code headers of members and implementations. Prose is deliberately out of
/// scope: a documentation comment may link anywhere, and a link in a sentence
/// discloses nothing. The owning item is the nearest anchor identifier before
/// the declaration.
///
/// `depth` is how many directories the rendered page sits below this crate's
/// own documentation root (0 for a page documenting a crate-root item); see
/// [`foreign`] for why a same-crate cross-module link needs it.
fn disclosures(prefix: &str, depth: usize, page: &str) -> BTreeSet<(String, String)> {
    const ANCHOR: &str = "id=\"";
    const LINK: &str = "href=\"";

    let rendered = declared(page);
    let mut found = BTreeSet::new();
    let mut anchor = String::new();
    let mut cursor = 0;

    while cursor < rendered.len() {
        let next_anchor = rendered[cursor..].find(ANCHOR).map(|at| cursor + at);
        let next_block =
            declaration(&rendered[cursor..]).map(|(at, end)| (cursor + at, cursor + end));

        match (next_anchor, next_block) {
            (Some(at), Some((start, _))) if at < start => {
                let value = at + ANCHOR.len();
                member(until(&rendered[value..], '"'))
                    .unwrap_or_default()
                    .clone_into(&mut anchor);
                cursor = value;
            }
            (_, Some((start, end))) => {
                let block = &rendered[start..end];
                for link in block.split(LINK).skip(1) {
                    if let Some(dependency) = foreign(until(link, '"'), depth) {
                        found.insert((item(prefix, &anchor), dependency));
                    }
                }
                cursor = end;
            }
            (Some(at), None) => {
                let value = at + ANCHOR.len();
                member(until(&rendered[value..], '"'))
                    .unwrap_or_default()
                    .clone_into(&mut anchor);
                cursor = value;
            }
            (None, None) => break,
        }
    }

    found
}

/// Locates the next rendered declaration as a `(start, end)` byte range.
///
/// Rustdoc renders an item's own signature in an `item-decl` block and every
/// member, trait item, and implementation header in a `code-header` element.
/// Between them they carry the argument types, return types, public fields,
/// associated types, and bounds a signature can disclose.
fn declaration(rest: &str) -> Option<(usize, usize)> {
    const BLOCKS: [(&str, &str); 2] = [
        ("class=\"rust item-decl\"", "</pre>"),
        ("class=\"code-header\"", "</h"),
    ];

    BLOCKS
        .iter()
        .filter_map(|(open, close)| {
            let start = rest.find(open)?;
            let end = rest[start..]
                .find(close)
                .map_or(rest.len(), |at| start + at + close.len());
            Some((start, end))
        })
        .min_by_key(|(start, _)| *start)
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

/// Returns what a link inside a declaration resolves to, if not to this crate.
///
/// Rustdoc addresses a documented dependency relatively and a dependency that
/// declares an `html_root_url` absolutely, so both forms are read. An absolute
/// link to any other host is reported under that host's name rather than
/// ignored: a rendered declaration should reach this crate, the standard
/// library, or a dependency, and a fourth kind of destination is something the
/// review has not seen. Failing on it costs one allowlist entry; ignoring it
/// would hide the next disclosure that arrives in an unfamiliar form.
///
/// `depth` (see [`disclosures`]) is what tells a same-crate, cross-module
/// relative link apart from a genuine one-hop link to a sibling *crate*: both
/// have the identical `../foo/...` shape when counted by leading `../`
/// segments alone, and only comparing that count against how deep the
/// *linking* page actually sits resolves the two. A link that walks up no
/// further than the page's own depth lands back at or inside this crate's
/// own rendered tree; only a link that walks up one level further than that
/// escapes it.
fn foreign(href: &str, depth: usize) -> Option<String> {
    const DOCS_RS: &str = "https://docs.rs/";
    const RUST: &str = "https://doc.rust-lang.org/";

    if let Some(rest) = href.strip_prefix(DOCS_RS) {
        return crate_name(until(rest, '/'));
    }
    if let Some(rest) = href.strip_prefix(RUST) {
        // The first segment is the toolchain version, the second the crate.
        return crate_name(until(
            rest.split_once('/').map_or(rest, |(_, tail)| tail),
            '/',
        ));
    }
    if let Some(host) = absolute_host(href) {
        return Some(host.to_owned());
    }

    let mut rest = href;
    let mut up_levels = 0usize;
    while let Some(parent) = rest.strip_prefix("../") {
        rest = parent;
        up_levels += 1;
    }
    if rest.contains("..") {
        // A `..` segment survives after the leading run was consumed --
        // this href is not the normalized "walk up N times, then descend"
        // shape every rustdoc-rendered relative link has, so the leading
        // `up_levels` count cannot be trusted to say whether it escapes this
        // crate. Report it rather than silently accept it: an unfamiliar
        // relative-link shape is exactly the case this scan cannot afford to
        // wave through (see the module doc's own disclosure philosophy).
        return Some("unnormalized-relative-link".to_owned());
    }
    if up_levels <= depth {
        return None;
    }
    if !rest.contains('/') {
        return None;
    }
    crate_name(until(rest, '/'))
}

/// Returns how many directories `page` sits below `root`.
///
/// A page documenting a crate-root item (`root/struct.Foo.html`) is at depth
/// `0`; a page nested under submodule directories (e.g.
/// `root/item_components/multi_resource/struct.X.html`) is at depth `2`. See
/// [`foreign`] for why [`disclosures`] needs this.
fn depth_of(page: &Path, root: &Path) -> usize {
    page.parent()
        .and_then(|directory| directory.strip_prefix(root).ok())
        .map_or(0, |relative| relative.components().count())
}

/// Borrows the host of an absolute link, when the link is absolute.
fn absolute_host(href: &str) -> Option<&str> {
    let rest = href
        .strip_prefix("https://")
        .or_else(|| href.strip_prefix("http://"))?;
    Some(until(rest, '/'))
}

/// Returns a crate-directory segment, when it names a crate this scan reports.
fn crate_name(segment: &str) -> Option<String> {
    let names_a_crate = !segment.is_empty()
        && segment
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_');

    if names_a_crate && !NOT_FOREIGN.contains(&segment) {
        Some(segment.to_owned())
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
    let accepted = ACCEPTED
        .iter()
        .map(|entry| (entry.item.to_owned(), entry.dependency.to_owned()))
        .collect();
    against(found, &accepted)
}

/// Reconciles what was found against a given set of accepted disclosures.
fn against(
    found: &BTreeSet<(String, String)>,
    accepted: &BTreeSet<(String, String)>,
) -> Vec<String> {
    let mut violations = Vec::new();
    for (item, dependency) in found.difference(accepted) {
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
    #![allow(clippy::expect_used, clippy::panic)]

    use super::{against, declared, depth_of, disclosures, foreign, item_prefix};
    use std::collections::BTreeSet;
    use std::fs;
    use std::path::{Path, PathBuf};

    /// Renders one member declaration the way rustdoc lays one out.
    fn method(anchor: &str, href: &str) -> String {
        format!(
            "<section id=\"{anchor}\" class=\"method\">\
             <h4 class=\"code-header\">pub fn <a href=\"#{anchor}\">member</a>() \
             -&gt; <a class=\"struct\" href=\"{href}\">Name</a></h4></section>"
        )
    }

    /// Collects the dependencies one page discloses, without their items.
    fn dependencies(prefix: &str, page: &str) -> BTreeSet<String> {
        disclosures(prefix, 0, page)
            .into_iter()
            .map(|(_, dependency)| dependency)
            .collect()
    }

    #[test]
    fn an_absolute_dependency_link_is_attributed_to_its_member() {
        let page = method(
            "method.manifest_value",
            "https://docs.rs/serde_json/1/serde_json/value/enum.Value.html",
        );

        assert_eq!(
            disclosures("oxide_batch::FlowTarget", 0, &page),
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
            disclosures("oxide_batch::JobLauncher", 0, &page),
            BTreeSet::from([(
                "oxide_batch::JobLauncher::handle".to_owned(),
                "tokio".to_owned()
            )])
        );
    }

    #[test]
    fn an_unknown_documentation_host_is_reported_rather_than_ignored() {
        let page = method(
            "method.session",
            "https://internal-docs.example/vendor_sdk/struct.Session.html",
        );

        assert_eq!(
            dependencies("oxide_batch::JobOperator", &page),
            BTreeSet::from(["internal-docs.example".to_owned()]),
            "a rendered declaration that leaves for an unfamiliar host is a \
             destination the review has not seen, not a link to skip",
        );
    }

    #[test]
    fn local_and_standard_library_links_are_not_disclosure() {
        assert_eq!(foreign("struct.JobName.html", 0), None);
        assert_eq!(foreign("#method.new", 0), None);
        assert_eq!(foreign("../oxide_batch/struct.JobName.html", 0), None);
        assert_eq!(foreign("../src/oxide_batch/lib.rs.html", 0), None);
        assert_eq!(foreign("../static.files/main.js", 0), None);
        assert_eq!(
            foreign(
                "https://doc.rust-lang.org/1.97.1/core/option/enum.Option.html",
                0
            ),
            None
        );
        assert_eq!(
            foreign(
                "https://doc.rust-lang.org/1.97.1/std/vec/struct.Vec.html",
                0
            ),
            None
        );
        assert_eq!(
            foreign("../sqlx/postgres/struct.PgPool.html", 0),
            Some("sqlx".to_owned())
        );
    }

    #[test]
    fn a_same_crate_cross_module_link_is_not_disclosure_regardless_of_leading_dotdot() {
        // A page two directories below the crate root (e.g.
        // `item_components/multi_resource/trait.Foo.html`) links to a sibling
        // submodule (`item_components/object_store/struct.Bar.html`) with
        // exactly one `../` -- the identical shape a crate-root page's
        // one-hop link to a genuinely foreign sibling *crate* has. Depth is
        // what tells them apart.
        assert_eq!(
            foreign("../object_store/struct.ObjectStoreReaderOpener.html", 2),
            None,
            "one `../` from a page nested two directories down still lands \
             back inside this crate's own rendered tree",
        );
        // Walking up exactly as many levels as the page is nested reaches
        // this crate's own root, not past it -- also not a disclosure.
        assert_eq!(
            foreign("../../struct.TopLevel.html", 2),
            None,
            "walking up exactly `depth` levels lands at this crate's own root"
        );
        // One level further than the page's own depth genuinely escapes this
        // crate's rendered tree, exactly like the depth-0 sibling-crate case.
        assert_eq!(
            foreign("../../../sqlx/postgres/struct.PgPool.html", 2),
            Some("sqlx".to_owned()),
            "walking up one level past the page's own depth escapes this \
             crate's rendered tree into a genuine sibling crate",
        );
    }

    #[test]
    fn depth_comparison_holds_at_deeper_nesting_than_the_motivating_case() {
        // The same three boundary cases as the depth-2 test above, one level
        // deeper (e.g. `item_components/multi_resource/inner/struct.X.html`,
        // depth 3), proving the depth comparison generalizes rather than
        // happening to work only at the one nesting level #150 introduced.
        assert_eq!(
            foreign("../sibling/struct.Y.html", 3),
            None,
            "one `../` from a page nested three directories down still \
             lands back inside this crate's own rendered tree",
        );
        assert_eq!(
            foreign("../../../struct.TopLevel.html", 3),
            None,
            "walking up exactly `depth` (3) levels lands at this crate's \
             own root, not past it"
        );
        assert_eq!(
            foreign("../../../../sqlx/postgres/struct.PgPool.html", 3),
            Some("sqlx".to_owned()),
            "walking up one level past a depth-3 page's own depth still \
             escapes this crate's rendered tree",
        );
    }

    #[test]
    fn a_relative_link_with_no_further_path_segment_is_not_disclosure() {
        // A link that walks up past its own depth but names no further
        // directory segment (bare filename, or an empty segment from a
        // malformed/non-canonical `../` run) cannot name a foreign crate --
        // `crate_name` requires a nonempty alphanumeric segment, so this
        // fails closed to "nothing resolvable" rather than panicking or
        // guessing a crate name from unexpected input.
        assert_eq!(foreign("../../bare-file.html", 1), None);
        assert_eq!(foreign("../../../.html", 1), None);
    }

    #[test]
    fn a_non_normalized_relative_link_fails_closed_rather_than_bypassing_the_scan() {
        // A `..` segment that survives after the leading run is consumed is
        // not the "walk up N times, then descend" shape any real rustdoc
        // output has. Before this fix, `up_levels` only counted the leading
        // run, so this shape left `up_levels == 0 <= depth` and slipped
        // through as `None` (not disclosure) regardless of what it actually
        // pointed at -- a fail-open hole for exactly the "malformed or
        // unfamiliar relative target" case the scan must fail closed on.
        assert_eq!(
            foreign("item_components/../../sqlx/struct.PgPool.html", 2),
            Some("unnormalized-relative-link".to_owned()),
        );
        assert_eq!(
            foreign("item_components/multi_resource/..", 2),
            Some("unnormalized-relative-link".to_owned()),
        );
    }

    #[test]
    fn prose_never_discloses() {
        let page = "<div class=\"docblock\"><p>See \
                    <a href=\"https://docs.rs/tokio/1/tokio/index.html\">tokio</a> \
                    for the executor this crate does not own.</p></div>";

        assert!(
            disclosures("oxide_batch::JobLauncher", 0, page).is_empty(),
            "a link in a sentence is a reference, not a signature",
        );
    }

    #[test]
    fn blanket_implementations_are_not_this_surface() {
        let page = format!(
            "<h2 id=\"blanket-implementations\">{}",
            method("method.into_either", "https://docs.rs/either/1/x.html")
        );

        assert!(!declared(&page).contains("either"));
        assert!(disclosures("oxide_batch::JobName", 0, &page).is_empty());
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
    fn depth_is_the_directory_nesting_below_the_crate_root() {
        let root = Path::new("target/doc/oxide_batch");
        assert_eq!(
            depth_of(
                Path::new("target/doc/oxide_batch/struct.JobName.html"),
                root
            ),
            0
        );
        assert_eq!(
            depth_of(
                Path::new("target/doc/oxide_batch/item_components/multi_resource/struct.X.html"),
                root
            ),
            2
        );
    }

    #[test]
    fn an_accepted_finding_that_is_gone_fails_as_loudly_as_a_new_one() {
        let accepted = BTreeSet::from([(
            "oxide_batch::Repaired::member".to_owned(),
            "serde_json".to_owned(),
        )]);

        let violations = against(&BTreeSet::new(), &accepted);

        assert_eq!(violations.len(), 1);
        assert!(violations[0].contains("remove its accepted entry"));
    }

    #[test]
    fn an_unlisted_disclosure_fails_against_an_allowlist() {
        let found = BTreeSet::from([("oxide_batch::New::member".to_owned(), "tokio".to_owned())]);

        let violations = against(&found, &BTreeSet::new());

        assert_eq!(violations.len(), 1);
        assert!(violations[0].contains("async runtime"));
    }

    #[test]
    fn every_signature_position_is_read_from_real_rustdoc_output() {
        let expected = [
            (
                "struct.Positions.html",
                "surface_fixture::Positions",
                // The item declaration carries the public field, and the two
                // members carry an argument type and a return type.
                vec![
                    "surface_fixture::Positions",
                    "surface_fixture::Positions::accept",
                    "surface_fixture::Positions::produce",
                ],
            ),
            (
                "trait.Bounded.html",
                "surface_fixture::Bounded",
                // The associated type carries a bound, and the required method
                // carries one on its own type parameter.
                vec![
                    "surface_fixture::Bounded::Document",
                    "surface_fixture::Bounded::describe",
                ],
            ),
        ];

        for (page, prefix, items) in expected {
            let rendered = fs::read_to_string(fixture(page))
                .unwrap_or_else(|error| panic!("could not read the {page} fixture: {error}"));
            let found = disclosures(prefix, 0, &rendered);

            for item in items {
                assert!(
                    found.contains(&(item.to_owned(), "serde_json".to_owned())),
                    "{item} discloses serde_json in real rustdoc output; \
                     the scan reported {found:?}",
                );
            }
        }
    }

    #[test]
    fn real_rustdoc_prose_and_blanket_sections_stay_out_of_the_scan() {
        let rendered = fs::read_to_string(fixture("struct.Positions.html"))
            .unwrap_or_else(|error| panic!("could not read the fixture: {error}"));

        assert!(
            rendered.contains("blanket-implementations"),
            "the fixture must contain the sections it proves are excluded",
        );
        assert_eq!(
            dependencies("surface_fixture::Positions", &rendered),
            BTreeSet::from(["serde_json".to_owned()]),
            "the ecosystem's blanket implementations and the crate-level prose \
             link contribute nothing",
        );
    }

    /// Locates one committed rustdoc fixture page.
    fn fixture(page: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("rustdoc")
            .join(page)
    }
}
